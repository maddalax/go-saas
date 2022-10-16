package bus

import "errors"

type Options struct {
	AddDefaultMiddleware bool
}

func CreateWithOptions[T any, V any](opts Options) EventBus[T, V] {
	bus := EventBus[T, V]{}
	if opts.AddDefaultMiddleware {
		bus.addDefaultMiddleware()
	}
	return bus
}

func Create[T any, V any]() EventBus[T, V] {
	return CreateWithOptions[T, V](Options{
		AddDefaultMiddleware: true,
	})
}

type EventBus[T any, V any] struct {
	listeners     []func(T) error
	middleware    []func(T)
	resultHandler func(T) (V, error)
}

func (bus *EventBus[T, V]) Listen(handler func(T) error) {
	if bus.listeners == nil {
		bus.listeners = make([]func(T) error, 0)
	}
	bus.listeners = append(bus.listeners, handler)
}

func (bus *EventBus[T, V]) addDefaultMiddleware() {
	middlewares := []func(T){
		eventLoggerMiddleware[T],
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
	for i := range bus.listeners {
		err := bus.listeners[i](payload)
		if err != nil {
			// TODO add error logging here
			continue
		}
	}
	return &result, nil
}
