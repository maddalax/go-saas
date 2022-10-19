package bus

import (
	"errors"
	"saas-starter/job"
)

type Options struct {
	AddDefaultMiddleware bool
	Workers              int
}

func CreateWithOptions[T any, V any](opts Options) EventBus[T, V] {
	bus := EventBus[T, V]{}
	if opts.AddDefaultMiddleware {
		bus.addDefaultMiddleware()
	}
	bus.queue = job.CreateQueue[T](job.CreateOptions{
		Workers: opts.Workers,
	})
	go bus.queue.Listen()
	return bus
}

func Create[T any, V any]() EventBus[T, V] {
	return CreateWithOptions[T, V](Options{
		AddDefaultMiddleware: true,
		Workers:              25,
	})
}

type EventBus[T any, V any] struct {
	queue         job.Queue[T]
	listeners     []func(T) error
	middleware    []func(T)
	resultHandler func(T) (V, error)
}

func (bus *EventBus[T, V]) Listen(handler func(T) error) {
	if bus.listeners == nil {
		bus.listeners = make([]func(T) error, 0)
	}

	bus.listeners = append(bus.listeners, handler)
	bus.queue.AddHandler(handler)
}

func (bus *EventBus[T, V]) addDefaultMiddleware() {
	middlewares := []func(T){
		consoleLoggerMiddleware[T],
		fileLoggerMiddleware[T],
	}
	bus.middleware = middlewares
}

func (bus *EventBus[T, V]) AddMiddleware(callback func(T)) {
	bus.middleware = append(bus.middleware, callback)
}

func (bus *EventBus[T, V]) SetHandler(handler func(T) (V, error)) {
	bus.resultHandler = handler
}

func (bus *EventBus[T, V]) Dispatch(payload T) (*V, error) {
	if bus.resultHandler == nil {
		return new(V), errors.New("result handler must be set")
	}

	for _, m := range bus.middleware {
		m(payload)
	}

	result, err := bus.resultHandler(payload)
	if err != nil {
		return new(V), err
	}

	bus.queue.Enqueue(payload)

	return &result, nil
}
