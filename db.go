package main

import (
	"context"
	"database/sql"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

type Book struct {
	Title string
}

func db() {
	dsn := "postgres://maddox:@localhost:5432/maddox?sslmode=disable"
	// dsn := "unix://user:pass@dbname/var/run/postgresql/.s.PGSQL.5432"
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))

	db := bun.NewDB(sqldb, pgdialect.New())

	_, err := db.NewCreateTable().Model(&Book{}).Table("books").IfNotExists().Exec(context.Background())

	if err != nil {
		panic(err)
	}

	book := &Book{Title: "hello"}

	_, err = db.NewInsert().Model(book).Exec(context.Background())

	println(err.Error())
}
