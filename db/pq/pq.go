package pq

import (
	"database/sql"

	"github.com/caiguanhao/furk/db"
	_ "github.com/lib/pq"
)

func Open(conn string) (db.DB, error) {
	c, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, err
	}
	if err := c.Ping(); err != nil {
		return nil, err
	}
	return &db.StandardDB{c}, nil
}
