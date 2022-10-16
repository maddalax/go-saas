package events

import "saas-starter/bus"

var CreateUser = bus.Create[CreateUserPayload, UserCreatedPayload]()
