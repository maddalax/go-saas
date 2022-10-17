package user

import (
	"saas-starter/events"
	"time"
)

func CreateHandler(payload events.CreateUserPayload) (events.UserCreatedPayload, error) {
	return events.UserCreatedPayload{
		Name: payload.Name,
	}, nil
}

type Book struct {
	Title string
}

func EmailHandler(payload events.CreateUserPayload) error {
	println("HERE!: " + payload.Name)
	time.Sleep(time.Millisecond * 1000)
	return nil
}
