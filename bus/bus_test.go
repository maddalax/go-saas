package bus

import (
	"errors"
	"testing"
)

type CreateUserPayload struct {
	Name string
}

type UserCreatedPayload struct {
	Name string
}

func TestDispatchAndGetResult(t *testing.T) {
	createUserEvent := CreateWithOptions[CreateUserPayload, UserCreatedPayload](Options{
		AddDefaultMiddleware: false,
	})

	createUserEvent.SetHandler(func(payload CreateUserPayload) (UserCreatedPayload, error) {
		return UserCreatedPayload{
			Name: "created user",
		}, nil
	})

	result, err := createUserEvent.Dispatch(CreateUserPayload{
		Name: "test",
	})

	if err != nil || result.Name != "created user" {
		t.Fail()
	}
}

func TestErrorIsReturned(t *testing.T) {
	createUserEvent := CreateWithOptions[CreateUserPayload, UserCreatedPayload](Options{
		AddDefaultMiddleware: false,
	})

	createUserEvent.SetHandler(func(payload CreateUserPayload) (UserCreatedPayload, error) {
		return UserCreatedPayload{}, errors.New("failed to create user")
	})

	_, err := createUserEvent.Dispatch(CreateUserPayload{
		Name: "test",
	})

	if err == nil {
		t.Fail()
	}
}

func TestMiddlewareAndListenersAreCalled(t *testing.T) {
	createUserEvent := CreateWithOptions[CreateUserPayload, UserCreatedPayload](Options{
		AddDefaultMiddleware: false,
	})

	createUserEvent.SetHandler(func(payload CreateUserPayload) (UserCreatedPayload, error) {
		return UserCreatedPayload{}, nil
	})

	calls := 0

	createUserEvent.AddMiddleware(func(data CreateUserPayload) {
		calls += 1
	})

	// Listener returning error should not interrupt the rest of the dispatches
	createUserEvent.Listen(func(data CreateUserPayload) error {
		calls += 1
		return errors.New("this is an error")
	})

	_, _ = createUserEvent.Dispatch(CreateUserPayload{
		Name: "test",
	})

	if calls != 2 {
		t.Fail()
	}
}
