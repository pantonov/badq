package badq

/*
BadQ: Pure Go, embeddable, persistent priority job queue, backed by Badger key-value database.

details: https://github.com/pantonov/badq
*/

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

// Return value of the job handler. May be PROC_RESULT_DONE, or PROC_RESULT_REQUEUE (put the job back at the end of the queue)
type JobResult int

const (
	JOB_RESULT_DONE    JobResult = 0 // Done, remove from queue
	JOB_RESULT_REQUEUE JobResult = 1 // Job not completed: re-queue at the end
)

type Options struct {
	// Max handlers running in parallel (default: 1)
	Concurrency uint

	// Write batch queue length (default: 1024)
	WriteBatchLen int

	// Key prefix in the database (default: nil). If you use prefixes, make sure they don't overlap!
	// (for example, empty prefix matches everything)
	KeyPrefix []byte

	// Use already opened database (default: open)
	Db *badger.DB

	// Badger options (default: badger.DefaultOptions)
	BadgerOptions badger.Options
}

func DefaultOptions(dbPath string, log badger.Logger) *Options {
	opt := &Options{
		Concurrency:   1,
		WriteBatchLen: 1024,
		BadgerOptions: badger.DefaultOptions(dbPath),
	}
	opt.BadgerOptions.SyncWrites = true
	if log != nil {
		opt.BadgerOptions.Logger = log
	}
	return opt
}

type kv struct {
	key   []byte
	value []byte
}

type BadQ struct {
	opt             *Options
	db              *badger.DB
	inq             chan (*kv)
	seq             atomic.Uint64
	jobs            map[uint64]bool
	jobsLock        sync.Mutex
	handler         JobFunc
	stopping        atomic.Bool
	runningHandlers sync.WaitGroup
	rescan          chan (bool)
	numJobs         atomic.Int64
}

// Job handler function. Must return PROC_RESULT_DONE or PROC_RESULT_REQUEUE.
type JobFunc = func(jobid uint64, job []byte) JobResult

// Create new BadQ instance with specified options and open persistence database.
func NewBadQWithOptions(opt *Options, pf JobFunc) *BadQ {
	bq := BadQ{
		opt: opt,
		inq: make(chan (*kv), opt.WriteBatchLen),
	}
	bq.stopping.Store(true)
	bq.handler = pf
	bq.jobs = make(map[uint64]bool)
	bq.rescan = make(chan bool, 1)
	return &bq
}

// Create typical BadQ instance with default options and open persistence database.
func NewBadQ(dbPath string, log badger.Logger, pf JobFunc) *BadQ {
	opt := DefaultOptions(dbPath, log)
	if log != nil {
		opt.BadgerOptions.Logger = log
	}
	return NewBadQWithOptions(opt, pf)
}

// Run values garbage collection
func (bq *BadQ) RunGC() {
	bq.db.RunValueLogGC(0.5)
}

// Enqueue job item with given priority. 0 is the highest priority, 255 the lowest
func (bq *BadQ) Push(prio uint8, job []byte) error {
	if bq.stopping.Load() {
		return fmt.Errorf("badq: instance is already stopped")
	}
	kp := bq.opt.KeyPrefix
	ki := bq.seq.Add(1)
	key := make([]byte, 8+len(kp))
	binary.BigEndian.PutUint64(key[len(kp):], ki)
	key[len(kp)] = prio
	copy(key, bq.opt.KeyPrefix)
	bq.inq <- &kv{key: key, value: job}
	return nil
}

func (bq *BadQ) toggleScan() {
	if bq.stopping.Load() {
		return
	}
	select {
	case bq.rescan <- true:
	default:
	}
}

// Open database, start accepting and executing jobs
func (bq *BadQ) Start() error {
	if bq.opt.Db != nil {
		bq.db = bq.opt.Db
	} else {
		if db, err := badger.Open(bq.opt.BadgerOptions); nil != err {
			return err
		} else {
			bq.db = db
			bq.RunGC()
		}
	}
	// get latest sequence number by scanning keys
	if err := bq.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = bq.opt.KeyPrefix
		it := txn.NewIterator(opts)
		defer it.Close()
		start_seq, num_jobs := uint64(0), int64(0)
		for it.Rewind(); it.Valid(); it.Next() {
			key_data := it.Item().Key()
			//bq.log().Infof("read key=%+v", key_data)
			_, seq, key_err := bq.decode_key(key_data)
			if nil != key_err {
				bq.log().Errorf("%s", key_err)
				continue // ignore bad key...
			}
			if start_seq < seq {
				start_seq = seq
			}
			num_jobs += 1
		}
		bq.seq.Store(start_seq + 1)
		bq.numJobs.Store(num_jobs)
		return nil
	}); nil != err {
		return err
	}
	bq.log().Infof("badq: number of stored jobs=%d, last job sequence=%d", bq.numJobs.Load(), bq.seq.Load())
	bq.stopping.Store(false)
	bq.runningHandlers.Add(2)
	go bq.runIn()
	go bq.runOut()
	return nil
}

// Returns number of pending messages for each priority
func (bq *BadQ) PrioStats() map[uint8]uint64 {
	stats := make(map[uint8]uint64)
	if bq.stopping.Load() {
		return stats // empty
	}
	bq.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = bq.opt.KeyPrefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key_data := it.Item().Key()
			prio, _, key_err := bq.decode_key(key_data)
			if nil != key_err {
				continue // ignore bad key...
			}
			stats[prio] += 1
		}
		return nil
	})
	return stats
}

// Stop processing and close the database, waiting for current processing functions to finish
func (bq *BadQ) Stop() {
	if !bq.stopping.CompareAndSwap(false, true) {
		return // already stopping
	}
	close(bq.rescan)
	close(bq.inq)
	bq.runningHandlers.Wait() // wait for inbound queue p rocessor and active proc funcs
	bq.db.Close()
}

// Returns true if badq is started
func (bq *BadQ) IsRunning() bool {
	return !bq.stopping.Load()
}

// Access to underlying database handle
func (bq *BadQ) Db() *badger.DB {
	return bq.db
}

func (bq *BadQ) runIn() {
	defer bq.runningHandlers.Done()
	for {
		vp, ok := <-bq.inq
		if !ok {
			return
		}
		if err := bq.db.Update(func(txn *badger.Txn) error {
			if err := txn.Set(vp.key, vp.value); nil != err {
				return err
			}
			bq.numJobs.Add(1)
			// coalesce remaining items in input queue
		qloop:
			for i := 0; len(bq.inq) > 0 && i < cap(bq.inq); i += 1 {
				select {
				case iv, iok := <-bq.inq:
					if !iok {
						break
					}
					if err := txn.Set(iv.key, iv.value); nil != err {
						return err
					}
					bq.numJobs.Add(1)
				default:
					break qloop
				}
			}
			return nil
		}); nil != err {
			bq.log().Errorf("badger enqueue error: %s", err)
			continue
		}
		bq.toggleScan()
	}
}

func (bq *BadQ) runOut() {
	defer bq.runningHandlers.Done()
	for {
		if err := bq.db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 1
			opts.Prefix = bq.opt.KeyPrefix
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				k := item.Key()
				prio, jobid, key_err := bq.decode_key(k)
				if nil != key_err {
					continue // ignore bad key, it will be reported on start-up
				}
				if func() bool {
					bq.jobsLock.Lock()
					defer bq.jobsLock.Unlock()
					if len(bq.jobs) >= int(bq.opt.Concurrency) {
						return true // too many handlers
					}
					if bq.jobs[jobid] {
						return true // job already running
					}
					bq.jobs[jobid] = true
					return false
				}() {
					break
				}
				vcp, err := item.ValueCopy(nil)
				if nil != err {
					return err
				}
				kcp := item.KeyCopy(nil)
				bq.runningHandlers.Add(1)
				go func() {
					defer func() {
						bq.jobsLock.Lock()
						delete(bq.jobs, jobid)
						bq.jobsLock.Unlock()
						bq.toggleScan()
						bq.runningHandlers.Done()
					}()
					switch bq.handler(jobid, vcp) {
					case JOB_RESULT_DONE:
						bq.delete_job(kcp)
					case JOB_RESULT_REQUEUE:
						bq.requeue_job(prio, kcp, vcp)
					default:
						panic("badq: unknown result value from handler function")
					}
				}()
			}
			return nil
		}); nil != err {
			bq.log().Errorf("badq out-queue: tx error: %s", err)
		}
		_, ok := <-bq.rescan
		if !ok {
			return
		}
	}
}

func (bq *BadQ) delete_job(k []byte) {
	if err := bq.db.Update(func(txn *badger.Txn) error {
		//bq.log().Infof("delete: key=%+v", k)
		return txn.Delete(k)
	}); nil != err {
		bq.opt.BadgerOptions.Logger.Errorf("badq: error while deleting key: %s", err)
	}
	bq.numJobs.Add(-1)
}

func (bq *BadQ) requeue_job(prio uint8, k, v []byte) {
	nk := make([]byte, 8)
	if copy(nk, k) != 8 {
		bq.log().Errorf("badq: invalid key len %d", len(k))
		return
	}
	nk[0] = prio
	if err := bq.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(k); nil != err {
			return err
		}
		return txn.Set(nk, v)
	}); nil != err {
		bq.log().Errorf("badq: error while requeueing job: %s", err)
	}
}

func (bq *BadQ) NumJobs() int64 {
	return bq.numJobs.Load()
}

func (bq *BadQ) log() badger.Logger {
	return bq.opt.BadgerOptions.Logger
}

func (bq *BadQ) decode_key(key_data []byte) (uint8, uint64, error) {
	if len(key_data) != 8+len(bq.opt.KeyPrefix) {
		return 0, 0, fmt.Errorf("badq: invalid key length '%d' in storage, key: %+v", len(bq.opt.KeyPrefix), bq.opt.KeyPrefix)
	}
	data := key_data[len(bq.opt.KeyPrefix):]
	v := binary.BigEndian.Uint64(data)
	return data[0], v & 0xffffffffffffff, nil
}
