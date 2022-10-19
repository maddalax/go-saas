package job

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/driver/pgdriver"
	"saas-starter/db"
	"time"
)

type RawJob struct {
	bun.BaseModel `bun:"table:jobs"`
	Id            string
	Name          string
	Payload       json.RawMessage `bun:"type:jsonb"`
	CreatedAt     string          `bun:"type:timestamp"`
	Tries         int
}

type Job[T any] struct {
	Id        string
	Name      string
	Payload   T
	CreatedAt string
	Tries     int
}

type Queue[T any] struct {
	listenerChan chan bool
	workers      int
	processors   []Processor[T]
	stop         chan bool
	context      context.Context
	cancel       context.CancelFunc
}

type CreateOptions struct {
	Workers int
}

// ListenerChanBuffer The total amount of messages the job change listener can hold into memory at once. These messages are
// just a notification to tell a worker to query for any new jobs, it does not contain any job data.
// This is useful because if we know that we have 2500 jobs to run, send these 2500 messages to the workers to tell them
// to query for new jobs 2500 times, therefore running all jobs without having for them to manually poll./*
const ListenerChanBuffer = 10000

func CreateQueue[T any](options CreateOptions) Queue[T] {

	if options.Workers == 0 {
		options.Workers = 25
	}

	ctx, cancel := context.WithCancel(context.Background())

	processors := make([]Processor[T], options.Workers)

	for i := range processors {
		processors[i] = createProcessor[T](i)
	}

	queue := Queue[T]{
		listenerChan: make(chan bool, ListenerChanBuffer),
		workers:      options.Workers,
		processors:   processors,
		context:      ctx,
		cancel:       cancel,
	}

	for _, processor := range processors {
		go processor.Start(queue.listenerChan)
	}

	queue.startPoller()

	return queue
}

func (queue Queue[T]) AddHandler(handler func(T) error) {
	for _, processor := range queue.processors {
		processor.AddHandler(handler)
	}
}

/*
*
Poll the jobs table every minute to check the count to see if there are any jobs
we need to process. This is useful for if messages get missed / dropped from the
pg_notify call.
*/
func (queue Queue[T]) startPoller() {
	go func() {
		for {
			println(fmt.Sprintf("len of listener chan: %d", len(queue.listenerChan)))
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for {
			count, err := db.GetDatabase().NewSelect().Model(&RawJob{}).Count(context.Background())
			if err != nil {
				println(err.Error())
				// TODO logging
				continue
			}
			diff := count - len(queue.listenerChan)
			// Ensure we don't send more than the max limit of the channel
			// TODO also we should drain the channel if its more than count and just set it to count
			if diff > ListenerChanBuffer {
				diff = ListenerChanBuffer
			}
			println(fmt.Sprintf("len of listener chan: %d, job count: %d, diff: %d", len(queue.listenerChan), count, diff))
			for i := 0; i < diff; i++ {
				queue.listenerChan <- true
			}
			time.Sleep(time.Minute)
		}
	}()
}

// Listen Start listening for pg changes for the exact event and notify our queue that the table has changed.
// The queue will notify the workers and a random worker will then know to pick up a new job/*
func (queue Queue[T]) Listen() {
	event := fmt.Sprintf("%T", *new(T))
	ln := pgdriver.NewListener(db.GetDatabase())
	key := "jobs:changed:" + event
	if err := ln.Listen(context.Background(), key); err != nil {
		panic(err)
	}
	c := queue.listenerChan
	// postgres jobs table has changed
	for range ln.Channel() {
		c <- true
	}
}

func (queue Queue[T]) Enqueue(payload T) {
	event := fmt.Sprintf("%T", payload)
	processes <- EnqueueJob{
		Name:    event,
		Payload: payload,
	}
}

func Initialize() error {
	startEnqueueListener()

	_, err := db.GetDatabase().NewCreateTable().Model(&RawJob{}).Table("jobs").IfNotExists().Exec(context.Background())
	if err != nil {
		return err
	}

	query := `
		BEGIN;

			CREATE OR REPLACE FUNCTION job_change_function()
				RETURNS TRIGGER AS
			$$
			BEGIN
				PERFORM pg_notify(concat('jobs:changed', ':', NEW.name), row_to_json(NEW)::text);
				RETURN NULL;
			END;
			$$
				LANGUAGE plpgsql;

			DROP TRIGGER IF EXISTS job_create_trigger ON jobs;
			DROP TRIGGER IF EXISTS job_update_trigger ON jobs;
			
			CREATE TRIGGER job_update_trigger
				AFTER UPDATE
				ON jobs
				FOR EACH ROW
			EXECUTE PROCEDURE job_change_function();
			
			CREATE TRIGGER job_create_trigger
				AFTER INSERT
				ON jobs
				FOR EACH ROW
			EXECUTE PROCEDURE job_change_function();

		COMMIT;
	`

	_, err = db.GetDatabase().Exec(query)

	if err != nil {
		return err
	}

	return nil
}
