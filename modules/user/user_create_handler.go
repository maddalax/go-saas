package user

import (
	"saas-starter/events"
)

func CreateHandler(payload events.CreateUserPayload) (events.UserCreatedPayload, error) {

	println(payload.Name)

	return events.UserCreatedPayload{
		Name: payload.Name,
	}, nil
}

func EmailHandler(payload events.CreateUserPayload) error {

	println("sending email: " + payload.Name)

	return nil
}
