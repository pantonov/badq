WIP

# badq
Extermely simple, embeddable, persistent work queue in pure Go, backed by Badger

# Features
 - 256 job priorities
 - Concurrent processing (concurrency level set via Options)
 - input job queue coalescing to reduce number of sync writes
 - support of job requeuing
 - optional key prefixes for re-using single database for multiple purposes
 - graceful shutdown

# Simple design
- job sequence numbers are non-persistent: initialized at start-up as highest stored job sequence in db
- priority is stored as a prefix, so stored key has format <prefix,priority,sequence>.

# Example
```go
        bq := NewBadQ("db_path", nil, func(jid uint64, job []byte) JobResult {
            fmt.Printf("Processed job with id %d: %s\n", jid, string(job))
            time.Sleep(time.Second)
            return JOB_RESULT_DONE
        })
        if err := bq.Start(); nil != err {
            log.Fatal(err)
        }
        bq.Push(1, "first")
        bq.Push(1, "second")
        bq.Push(1, "third")
        bq.push(0, "fourth") // will be processed with higher priority
```

# License
 Whatever

