package job

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/uptrace/bun"
	"os"
	"saas-starter/db"
	"time"
)

// channelBuffer The total amount of messages the job change listener can hold into memory at once. These messages are
// just a notification to tell a worker to query for any new jobs, it does not contain any job data.
// This is useful because if we know that we have 2500 jobs to run, send these 2500 messages to the workers to tell them
// to query for new jobs 2500 times, therefore running all jobs without having for them to manually poll./*
const channelBuffer = 10000

// The amount of jobs we would like to query at once.
const prefetchLimit = 500

type PollerHandler struct {
	event   string
	handler chan RawJob
}

type Poller struct {
	id          string
	channel     chan bool
	handlers    map[string]chan RawJob
	handlerChan chan PollerHandler
}

func CreatePoller() Poller {
	hostname, _ := os.Hostname()
	return Poller{
		// id that is used to set the lockedBy column on the job table, so we know which process is handling which jobs
		// TODO change hostname to something like machineId -> https://github.com/denisbrodbeck/machineid
		id:          fmt.Sprintf(hostname),
		channel:     make(chan bool, channelBuffer),
		handlers:    make(map[string]chan RawJob, 0),
		handlerChan: make(chan PollerHandler, 1000),
	}
}

func (poller Poller) Start() {
	go poller.poll()
	go poller.listen()
}

// Poll the jobs table every minute to check the count to see if there are any jobs
// we need to process. This is useful for if messages get missed / dropped from the
// pg_notify call./*
func (poller Poller) poll() {
	for {
		count, err := db.GetDatabase().NewSelect().Model(&RawJob{}).Where("status = ?", "pending").Count(context.Background())
		if err != nil {
			println(err.Error())
			// TODO logging
			continue
		}

		// Drain the channel to match count
		if len(poller.channel) > count {
			for len(poller.channel) > count {
				<-poller.channel
			}
		}

		diff := count - len(poller.channel)
		// Ensure we don't send more than the max limit of the channel
		if diff > channelBuffer {
			diff = channelBuffer
		}
		for i := 0; i < diff; i++ {
			poller.channel <- true
		}
		time.Sleep(time.Minute)
	}
}

func (poller Poller) listen() {
	prefetchTimeout := 5 * time.Second
	messages := 0
	tick := time.NewTicker(prefetchTimeout)

	handlers := make(map[string]chan RawJob, 0)

	for {
		select {

		case handler := <-poller.handlerChan:
			handlers[handler.event] = handler.handler

		// Check if a new messages is available.
		// If so, store it and check if the messages
		// has exceeded its size limit.
		case _ = <-poller.channel:
			messages += 1

			if messages < prefetchLimit {
				break
			}

			// Reset the timeout ticker.
			// Otherwise we will get too many sends.
			tick.Stop()

			// Send the cached messages and reset the messages.
			poller.sendNextJobs(messages, handlers)
			messages = 0

			// Recreate the ticker, so the timeout trigger
			// remains consistent.
			tick = time.NewTicker(prefetchTimeout)

		// If the timeout is reached, send the
		// current message messages, regardless of
		// its size.
		case <-tick.C:
			poller.sendNextJobs(messages, handlers)
			messages = 0
		}
	}
}

func (poller Poller) addHandler(handler PollerHandler) {
	poller.handlerChan <- handler
}

func (poller Poller) sendNextJobs(count int, handlers map[string]chan RawJob) {

	if count > prefetchLimit {
		count = prefetchLimit
	}

	if count == 0 {
		count = prefetchLimit
	}

	err := db.GetDatabase().RunInTx(context.Background(), &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.Exec("BEGIN")
		if err != nil {
			return err
		}

		inProgress, err := tx.NewSelect().Model(&RawJob{}).Where("locked_by = ? and status = ?", poller.id, "running").Count(context.Background())

		if err != nil {
			return err
		}

		diff := prefetchLimit - inProgress

		if count > diff {
			count = diff
		}

		if count <= 0 {
			count = 0
		}

		println(fmt.Sprintf("querying for %d jobs to run.", count))

		var jobs []RawJob

		err = tx.NewRaw(`
		UPDATE jobs
		SET status = 'running', locked_by = ?
		FROM (SELECT * FROM jobs WHERE status = 'pending' LIMIT ? FOR UPDATE SKIP LOCKED) AS subquery
		WHERE jobs.id = subquery.id
		RETURNING subquery.*;
		`, poller.id, diff).Scan(ctx, &jobs)

		for _, job := range jobs {
			handler := handlers[job.Name]
			if handler != nil {
				handler <- job
			}
		}

		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		println(err.Error())
	}
}
