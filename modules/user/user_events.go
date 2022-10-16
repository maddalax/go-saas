package user

import "saas-starter/events"

func RegisterEvents() {
	events.CreateUser.Listen(EmailHandler)
	events.CreateUser.SetHandler(CreateHandler)
}
