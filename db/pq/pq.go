package pq

import (
	"database/sql"

	"github.com/caiguanhao/furk/db"
	"github.com/caiguanhao/furk/db/standard"
	_ "github.com/lib/pq"
)

// MustOpen is like Open but panics if connect operation fails.
func MustOpen(conn string) db.DB {
	c, err := Open(conn)
	if err != nil {
		panic(err)
	}
	return c
}

// Open creates and establishes one connection to database.
func Open(conn string) (db.DB, error) {
	c, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}
	if err := c.Ping(); err != nil {
		return nil, err
	}
	return &standard.DB{c}, nil
}
