package bus

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func consoleLoggerMiddleware[T any](value T) {
	serialized, err := json.Marshal(value)
	if err != nil {
		println(err)
	}
	log.Println(fmt.Sprintf("%T %s\n", value, serialized))
}

func fileLoggerMiddleware[T any](value T) {
	f, err := os.OpenFile("events.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	logger := log.New(f, fmt.Sprintf("%T", value)+" ", log.LstdFlags)
	serialized, err := json.Marshal(value)
	logger.Println(string(serialized))
}
