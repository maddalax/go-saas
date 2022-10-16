package user

import (
	"context"
	"saas-starter/db"
	"saas-starter/events"
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
	book := &Book{Title: payload.Name}

	_, err := db.GetDatabase().NewInsert().Model(book).Exec(context.Background())

	if err != nil {
		return err
	}

	return nil
}
