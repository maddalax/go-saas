package job

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"log"
	"saas-starter/db"
	"time"
)

/*
*
Maximum number of jobs that can be run in parallel
*/
const maxConcurrency = 50

/*
*
Maximum number of jobs that can be sitting in the queue
before it stops allowing more
*/
const maxProcesses = 10000

/*
*
Number of times a specific job will retry
*/
const maxRetries = 15

var semaphore = make(chan bool, maxConcurrency)

type EnqueueJob struct {
	Name    string
	Payload interface{}
	Tries   int
}

var processes = make(chan EnqueueJob, maxProcesses)

func startEnqueueListener() {
	for i := 0; i < maxConcurrency; i++ {
		semaphore <- true
	}
	for i := 0; i < maxConcurrency; i++ {
		go func() {
			for {
				request := <-processes
				println(fmt.Sprintf("workerChannel: %d, workerSempahore: %d", len(processes), len(semaphore)))
				serialized, err := json.Marshal(request.Payload)
				if err != nil {
					println(err.Error())
					continue
				}
				job := RawJob{
					Id:        uuid.NewString(),
					Name:      request.Name,
					Payload:   serialized,
					CreatedAt: time.Now().Format(time.RFC3339),
					Tries:     0,
				}
				_, err = db.GetDatabase().NewInsert().Model(&job).Exec(context.Background())

				if err != nil {

					log.Default().Print(err)

					if request.Tries > maxRetries {
						log.Default().Print(err)
						continue
					}

					request.Tries += 1
					processes <- request
				}
			}
		}()
	}
}
