package main

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"saas-starter/events"
	"saas-starter/modules/user"
)

func main() {

	user.RegisterEvents()

	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		result, err := events.CreateUser.Dispatch(events.CreateUserPayload{Name: "hello"})

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
