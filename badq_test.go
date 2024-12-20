package badq

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

const total_jobs = 100000

var completedJobs = atomic.Int64{}

func testpf(jid uint64, v []byte) JobResult {
	completedJobs.Add(1)
	return JOB_RESULT_DONE
}

type V struct {
	V string
}

func Test1(t *testing.T) {
	opt := DefaultOptions("testdb", nil)
	opt.Concurrency = 5
	q := NewBadQWithOptions(opt, testpf)
	if err := q.Start(); nil != err {
		t.Fatal(err)
	}
	for i := 0; i < total_jobs; i += 1 {
		prio := i % 111 // enqueue with different priorities (scattered write)
		q.Push(uint8(prio), []byte(fmt.Sprintf("job_seq=%d,prio=%d", i, prio)))
	}
	// set a upper time limit
	for cnt := 0; completedJobs.Load() != total_jobs && cnt < total_jobs/100; cnt += 1 {
		time.Sleep(10 * time.Millisecond)
	}
	q.Stop()
	if completedJobs.Load() != total_jobs {
		t.Fatalf("completed jobs number mismatch %d != %d", completedJobs.Load(), total_jobs)
	}
}
