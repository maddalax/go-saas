package events

type CreateUserPayload struct {
	Name string
}

type UserCreatedPayload struct {
	Id   string
	Name string
}
