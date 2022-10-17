package job

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/driver/pgdriver"
	"saas-starter/db"
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
	maxWorkers int
	maxBuffer  int
	semaphore  chan bool
	workers    chan bool
	handlers   []func(T) error
	stop       chan bool
	context    context.Context
	cancel     context.CancelFunc
}

func CreateQueue[T any]() Queue[T] {
	maxWorkers := 100
	maxBuffer := 10000
	ctx, cancel := context.WithCancel(context.Background())
	queue := Queue[T]{
		maxWorkers: maxWorkers,
		maxBuffer:  10000,
		semaphore:  make(chan bool, maxWorkers),
		workers:    make(chan bool, maxBuffer),
		handlers:   make([]func(T) error, 0),
		context:    ctx,
		cancel:     cancel,
	}
	queue.startWorkers(queue.context)
	return queue
}

func (queue Queue[T]) AddHandler(handler func(T) error) {
	queue.handlers = append(queue.handlers, handler)
	// TODO logging
	println("handlers changed, restarting workers.")
	queue.restartWorkers()
}

func (queue Queue[T]) restartWorkers() {
	queue.cancel()
	ctx, cancel := context.WithCancel(context.Background())
	queue.context = ctx
	queue.cancel = cancel
	for len(queue.semaphore) > 0 {
		<-queue.semaphore
	}
	queue.startWorkers(queue.context)
}

func (queue Queue[T]) Listen() {
	event := fmt.Sprintf("%T", *new(T))
	ln := pgdriver.NewListener(db.GetDatabase())
	key := "jobs:changed:" + event
	println("adding pg job listener: " + key)
	if err := ln.Listen(context.Background(), key); err != nil {
		panic(err)
	}
	for range ln.Channel() {
		queue.workers <- true
	}
}

func (queue Queue[T]) startWorkers(ctx context.Context) {
	for i := 0; i < queue.maxWorkers; i++ {
		queue.semaphore <- true
	}
	for i := 0; i < queue.maxWorkers; i++ {
		go func(ctx context.Context) {
			for {
				select {
				case <-queue.workers:
					queue.process()
				case <-ctx.Done():
					return
				}
			}
		}(ctx)
	}
}

func (queue Queue[T]) process() {
	println("grabbing job")
	err := db.GetDatabase().RunInTx(context.Background(), &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		_, err := tx.Exec("BEGIN")
		if err != nil {
			return err
		}
		raw := RawJob{}
		err = tx.NewRaw("SELECT * FROM jobs LIMIT 1 FOR UPDATE SKIP LOCKED;").Scan(ctx, &raw)
		if err != nil {
			return err
		}

		job, err := queue.rawToJob(raw)

		if err != nil {
			return err
		}

		err = queue.doProcessJob(job)

		if err != nil {
			return err
		}

		_, err = tx.NewDelete().Model(&RawJob{}).Where("id = ?", job.Id).Exec(ctx)
		return err
	})

	if err != nil {
		println(err.Error())
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

func (queue Queue[T]) rawToJob(raw RawJob) (Job[T], error) {
	job := Job[T]{
		Id:        raw.Id,
		Name:      raw.Name,
		CreatedAt: raw.CreatedAt,
		Tries:     raw.Tries,
	}
	value := new(T)
	err := json.Unmarshal(raw.Payload, &value)
	if err != nil {
		return Job[T]{}, err
	}
	job.Payload = *value
	return job, nil
}

func (queue Queue[T]) doProcessJob(job Job[T]) error {
	for _, f := range queue.handlers {
		err := f(job.Payload)
		if err != nil {
			return err
		}
	}
	return nil
}
