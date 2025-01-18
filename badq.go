package badq

/*
BadQ: Pure Go, embeddable, persistent priority job queue, backed by Badger key-value database.

details: https://github.com/pantonov/badq
*/

import (
	"encoding/binary"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
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

	WriteBatchLen int

	// Disable badger sync writes
	DisableSyncWrites bool

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
	q               *rbt.Tree
	qlock           sync.Mutex
	handler         JobFunc
	stopping        atomic.Bool
	runningHandlers sync.WaitGroup
	rescan          chan (bool)
	numJobs         atomic.Int64
	outWait         chan (bool)
	dbWait          chan (bool)
}

// Job handler function. Must return PROC_RESULT_DONE or PROC_RESULT_REQUEUE.
type JobFunc = func(jobid uint64, job []byte) JobResult

// Create new BadQ instance with specified options and open persistence database.
func NewBadQWithOptions(opt *Options, pf JobFunc) *BadQ {
	bq := BadQ{
		opt: opt,
		inq: make(chan (*kv), opt.WriteBatchLen),
	}
	opt.BadgerOptions.SyncWrites = !opt.DisableSyncWrites
	bq.q = rbt.NewWith(func(a, b interface{}) int { return slices.Compare(a.([]byte), b.([]byte)) })
	bq.stopping.Store(true)
	bq.handler = pf
	bq.rescan = make(chan bool, 1)
	bq.outWait = make(chan bool, 1)
	bq.dbWait = make(chan bool, 1)
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
	if bq.stopping.Load() {
		return
	}
	bq.db.RunValueLogGC(0.5)
}

// Enqueue job item with given priority. 0 is the highest priority, 255 the lowest
// Returns unique job id in queue
func (bq *BadQ) Push(prio uint8, job []byte) (uint64, error) {
	if bq.stopping.Load() {
		return 0, fmt.Errorf("badq: instance is already stopped")
	}
	kp := bq.opt.KeyPrefix
	ki := bq.seq.Add(1)
	key := make([]byte, 8+len(kp))
	binary.BigEndian.PutUint64(key[len(kp):], ki)
	key[len(kp)] = prio
	copy(key, bq.opt.KeyPrefix)
	bq.inq <- &kv{key: key, value: job}
	bq.qlock.Lock()
	bq.q.Put(key, job)
	bq.qlock.Unlock()
	bq.numJobs.Add(1)
	return ki, nil
}

// todo: use len(concuurrency)
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
	bq.qlock.Lock()
	defer bq.qlock.Unlock()
	// get latest sequence number by scanning keys
	if err := bq.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = bq.opt.KeyPrefix
		it := txn.NewIterator(opts)
		defer it.Close()
		start_seq, num_jobs := uint64(0), int64(0)
		for it.Rewind(); it.Valid(); it.Next() {
			key_data := it.Item().KeyCopy(nil)
			//bq.log().Infof("read key=%+v", key_data)
			_, seq, err := bq.decode_key(key_data)
			if nil != err {
				bq.log().Errorf("%s", err)
				continue // ignore bad key...
			}
			if v, err := it.Item().ValueCopy(nil); nil != err {
				bq.log().Errorf("%s", err)
				continue
			} else {
				bq.q.Put(key_data, v)
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
	go bq.runIn()
	go bq.runOut()
	for i := 0; i < int(bq.opt.Concurrency); i += 1 {
		bq.toggleScan()
	}
	return nil
}

// Returns number of pending messages for each priority
func (bq *BadQ) PrioStats() map[uint8]uint64 {
	stats := make(map[uint8]uint64)
	if bq.stopping.Load() {
		return stats // empty
	}
	bq.qlock.Lock()
	defer bq.qlock.Unlock()
	for it := bq.q.Iterator(); it.Next(); {
		prio, _, key_err := bq.decode_key(it.Key().([]byte))
		if nil != key_err {
			continue // ignore bad key...
		}
		stats[prio] += 1
	}
	return stats
}

// Stop processing and close the database, waiting for current processing functions to finish
func (bq *BadQ) Stop() {
	if !bq.stopping.CompareAndSwap(false, true) {
		return // already stopping
	}
	close(bq.rescan)
	bq.runningHandlers.Wait() // wait for running handlers
	<-bq.outWait
	bq.inq <- &kv{}
	<-bq.dbWait
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
	defer func() {
		bq.dbWait <- true
	}()
	stop := false
	for {
		vp := <-bq.inq
		if vp.key == nil { // flush and return
			return
		}
		if err := bq.db.Update(func(txn *badger.Txn) error {
			update_or_del := func(vv *kv) error {
				if vv.value == nil {
					return txn.Delete(vv.key)
				} else {
					return txn.Set(vv.key, vv.value)
				}
			}
			if err := update_or_del(vp); nil != err {
				return err
			}
			// coalesce remaining items in input queue
		qloop:
			for i := 0; len(bq.inq) > 0 && i < cap(bq.inq); i += 1 {
				select {
				case iv, iok := <-bq.inq:
					if !iok || iv.key == nil {
						stop = true
						break
					}
					if err := update_or_del(iv); nil != err {
						return err
					}
				default:
					break qloop
				}
			}
			return nil
		}); nil != err {
			bq.log().Errorf("badger update error: %s", err)
		}
		if stop {
			return
		}
	}
}

func (bq *BadQ) runOut() {
	defer func() {
		bq.outWait <- true
	}()
	tickets := make(chan bool, bq.opt.Concurrency)
	for i := 0; i < int(bq.opt.Concurrency); i += 1 {
		tickets <- true
	}
	for {
		if bq.stopping.Load() {
			return
		}
		_, ok := <-bq.rescan
		if !ok {
			return
		}
		for {
			bq.qlock.Lock()
			node := bq.q.Left()
			if nil == node {
				bq.qlock.Unlock()
				break
			}
			key := node.Key.([]byte)
			value := node.Value.([]byte)
			bq.q.Remove(node.Key)
			bq.qlock.Unlock()
			bq.numJobs.Add(-1)
			<-tickets
			if bq.stopping.Load() {
				break
			}
			bq.inq <- &kv{key: key} // request db deletion
			bq.runningHandlers.Add(1)
			go func() {
				prio, jobid, _ := bq.decode_key(key)
				defer func() {
					if r := recover(); r != nil {
						bq.log().Errorf("badq handler recover, job id %d: stack trace:\n%s", jobid, string(debug.Stack()))
					}
					bq.toggleScan()
					tickets <- true
					bq.runningHandlers.Done()
				}()
				switch bq.handler(jobid, value) {
				case JOB_RESULT_DONE:
				case JOB_RESULT_REQUEUE:
					bq.Push(prio, value)
				default:
					panic("badq: unknown result value from handler function")
				}
			}()
		}
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
