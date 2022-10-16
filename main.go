package main

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"net/http"
	"saas-starter/bus"
	"saas-starter/events"
	"saas-starter/modules/user"
)

func main() {

	bus.StartQueue()
	user.RegisterEvents()

	r := gin.New()
	r.GET("/ping", func(c *gin.Context) {
		result, err := events.CreateUser.Dispatch(events.CreateUserPayload{Name: uuid.NewString()})

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
		} else {
			c.JSON(http.StatusOK, result)
		}
	})
	err := r.Run()
	if err != nil {
		panic(err)
	}
}
