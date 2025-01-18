package badq

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

const total_jobs = 100000

var completedJobs = atomic.Int64{}
var maxConc = atomic.Int64{}
var conc = atomic.Int64{}

func testpf(jid uint64, v []byte) JobResult {
	conc.Add(1)
	if maxConc.Load() < conc.Load() {
		maxConc.Store(conc.Load())
	}
	defer conc.Add(-1)
	time.Sleep(time.Microsecond) // do the work
	completedJobs.Add(1)
	return JOB_RESULT_DONE
}

type V struct {
	V string
}

func Test1(t *testing.T) {
	opt := DefaultOptions("testdb", nil)
	opt.Concurrency = 11
	q := NewBadQWithOptions(opt, testpf)
	if err := q.Start(); nil != err {
		t.Fatal(err)
	}
	for i := 0; i < total_jobs; i += 1 {
		prio := i % 111 // enqueue with different priorities (scattered write)
		q.Push(uint8(prio), []byte(fmt.Sprintf("%d %d", i, prio)))
	}
	// set a upper time limit
	time.Sleep(1 * time.Second)
	q.Stop()
	if maxConc.Load() != int64(opt.Concurrency) {
		t.Fatalf("concurrency mismatch: maxconc=%d", maxConc.Load())
	}
	if completedJobs.Load() != total_jobs {
		t.Fatalf("completed jobs number mismatch %d != %d", completedJobs.Load(), total_jobs)
	}
}
