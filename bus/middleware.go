package bus

import (
	"encoding/json"
	"log"
)

func eventLoggerMiddleware[T any](value T) {
	serialized, err := json.Marshal(value)
	if err != nil {
		println(err)
	}
	log.Println(string(serialized))
}
