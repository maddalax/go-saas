package job

import (
	"context"
	"fmt"
)

type Queue[T any] struct {
	listenerChan chan RawJob
	workers      int
	processors   []Processor[T]
	stop         chan bool
	context      context.Context
	cancel       context.CancelFunc
}

type CreateOptions struct {
	Workers int
}

type PgChangeEvent struct {
	Event   string
	Payload any
}

var poller = CreatePoller()
var notify = CreatePgNotify(poller)

func CreateQueue[T any](options CreateOptions) Queue[T] {

	if options.Workers == 0 {
		options.Workers = 150
	}

	ctx, cancel := context.WithCancel(context.Background())

	processors := make([]Processor[T], options.Workers)

	for i := range processors {
		processors[i] = createProcessor[T](i)
	}

	queue := Queue[T]{
		listenerChan: make(chan RawJob, 10000),
		workers:      options.Workers,
		processors:   processors,
		context:      ctx,
		cancel:       cancel,
	}

	for _, processor := range processors {
		go processor.Start(queue.listenerChan)
	}

	return queue
}

func (queue Queue[T]) AddHandler(handler func(T) error) {
	for _, processor := range queue.processors {
		processor.AddHandler(handler)
	}
}

// Listen Start listening for pg changes for the exact event and pgNotify our queue that the table has changed.
// The queue will pgNotify the workers and a random worker will then know to pick up a new job/*
func (queue Queue[T]) Listen() {
	event := fmt.Sprintf("%T", *new(T))
	poller.addHandler(PollerHandler{
		event:   event,
		handler: queue.listenerChan,
	})
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
	poller.Start()
	return notify.Start()
}
