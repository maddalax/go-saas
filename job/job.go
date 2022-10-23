package job

import (
	"context"
	"encoding/json"
	"github.com/uptrace/bun"
	"saas-starter/db"
	"time"
)

type RawJob struct {
	bun.BaseModel `bun:"table:jobs"`
	Id            string
	Name          string
	Payload       json.RawMessage `bun:"type:jsonb"`
	CreatedAt     string          `bun:"type:timestamp"`
	Tries         int
	Status        string
	LockedBy      string
	LastPing      string
}

type Job[T any] struct {
	Id        string
	Name      string
	Payload   T
	CreatedAt string
	Tries     int
	Status    string
	LockedBy  string
}

func (job Job[T]) Complete() {
	_, err := db.GetDatabase().NewDelete().Model(&RawJob{}).Where("id = ?", job.Id).Exec(context.Background())
	if err != nil {
		println(err.Error())
		// TODO logging
	}
}

func (job Job[T]) Fail() {
	_, err := db.GetDatabase().NewUpdate().Model(&RawJob{}).Set("status = ?, locked_by = ?", "failed", nil).Where("id = ?", job.Id).Exec(context.Background())
	if err != nil {
		println(err.Error())
		// TODO logging
	}
}

// Ping
//
//	Updates the table to let the queue know that the job is still being processed.
//	The job should be updated every 30s. If 30s has gone by and the job has not been updated,
//	we can assume it is dead and need to be re-run/**
func (job Job[T]) Ping() {
	_, err := db.GetDatabase().NewUpdate().Model(&RawJob{}).Set("last_ping = ?", time.Now()).Where("id = ?", job.Id).Exec(context.Background())
	if err != nil {
		println(err.Error())
		// TODO logging
	}
}
