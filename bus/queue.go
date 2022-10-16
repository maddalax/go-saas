package bus

import (
	"fmt"
	"log"
)

/*
*
Maximum number of jobs that can be run in parallel
*/
const maxConcurrency = 100

/*
*
Maximum number of jobs that can be sitting in the queue
before it stops allowing more
*/
const maxProcesses = 100000

/*
*
Number of times a specific job will retry
*/
const maxRetries = 3

var Semaphore = make(chan bool, maxConcurrency)
var Processes = make(chan Job, maxProcesses)

type Job struct {
	callback func() error
	tries    int
	error    error
}

func StartQueue() {
	for i := 0; i < maxConcurrency; i++ {
		Semaphore <- true
	}
	for i := 0; i < maxConcurrency; i++ {
		go func() {
			for {
				job := <-Processes
				println(fmt.Sprintf("Processes: %d, Semaphore: %d", len(Processes), len(Semaphore)))
				err := job.callback()
				if err != nil {
					if job.tries >= maxRetries {
						log.Default().Fatal(err)
						return
					}
					job.tries += 1
					job.error = err
					Processes <- job
				}
			}
		}()
	}
}
