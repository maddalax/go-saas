package job

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/uptrace/bun"
	"saas-starter/db"
	"sync"
)

func createProcessor[T any](index int) Processor[T] {
	return Processor[T]{
		handlerChan: make(chan func(T) error),
		index:       index,
	}
}

type Processor[T any] struct {
	index       int
	handlers    []func(T) error
	handlerChan chan func(T) error
}

func (processor Processor[T]) AddHandler(handler func(T) error) {
	processor.handlerChan <- handler
}

func (processor Processor[T]) Start(process chan bool) {
	for {
		select {
		case h := <-processor.handlerChan:
			println("appending handler")
			processor.handlers = append(processor.handlers, h)
			break
		case _ = <-process:
			processor.executeNextJob()
			break
		}
	}
}

func (processor Processor[T]) executeNextJob() {
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

		job, err := processor.rawToJob(raw)

		if err != nil {
			return err
		}

		err = processor.doProcessJob(job)

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

func (processor Processor[T]) rawToJob(raw RawJob) (Job[T], error) {
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

func (processor Processor[T]) doProcessJob(job Job[T]) error {
	var wg sync.WaitGroup

	for _, f := range processor.handlers {
		wg.Add(1)
		f := f
		go func() {
			defer wg.Done()
			err := f(job.Payload)
			if err != nil {
				// TODO re-add to quue if failure
			}
		}()
	}

	wg.Wait()

	return nil
}
