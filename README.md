[![](https://godoc.org/github.com/caiguanhao/furk?status.svg)](https://pkg.go.dev/github.com/caiguanhao/furk#section-directories)
[![Build Status](https://travis-ci.com/caiguanhao/furk.svg?branch=master)](https://travis-ci.com/caiguanhao/furk)

# furk

## db.Model

```go
package main

import (
	"database/sql"
	"fmt"

	"github.com/caiguanhao/furk/db"
	"github.com/caiguanhao/furk/logger"
	_ "github.com/lib/pq"
)

type (
	User struct {
		Id      int
		Name    string
		Picture string `jsonb:"meta"`
	}
)

func main() {
	c, err := sql.Open("postgres", "postgres://localhost:5432/test?sslmode=disable")
	if err != nil {
		panic(err)
	}
	if err := c.Ping(); err != nil {
		panic(err)
	}
	var conn db.DB = &db.StandardDB{c}

	user := db.NewModel(User{})
	user.SetConnection(conn)
	user.SetLogger(logger.StandardLogger)

	// DROP TABLE IF EXISTS users
	if err = user.NewSQLWithValues(user.DropSchema()).Execute(); err != nil {
		panic(err)
	}

	// CREATE TABLE users (
	//         id SERIAL PRIMARY KEY,
	//         status text DEFAULT ''::text NOT NULL,
	//         meta jsonb
	// )
	if err = user.NewSQLWithValues(user.Schema()).Execute(); err != nil {
		panic(err)
	}

	var id int
	// INSERT INTO users (name, meta) VALUES ($1, $2) RETURNING id [hello {"picture":"world!"}]
	err = user.Insert(
		user.Permit("Name", "Picture").Filter(`{ "Name": "hello", "Picture": "world!" }`),
	)("RETURNING id").QueryRow(&id)
	if err != nil {
		panic(err)
	}
	fmt.Println(id) // 1

	var firstUser User
	err = user.Find("WHERE id = $1", id).Query(&firstUser)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", firstUser) // {Id:1 Name:hello Picture:world!}

	var rowsAffected int
	// UPDATE users SET name = $1, meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', $2) [HELLO "WORLD!"]
	err = user.Update(
		user.Permit("Name", "Picture").Filter(`{ "Name": "HELLO", "Picture": "WORLD!" }`),
	)().Execute(&rowsAffected)
	fmt.Println(rowsAffected) // 1

	var users []User
	err = user.Find("ORDER BY id DESC").Query(&users)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", users) // [{Id:1 Name:HELLO Picture:WORLD!}]

	// SELECT COUNT(*) FROM users
	count, err := user.Count()
	if err != nil {
		panic(err)
	}
	fmt.Println(count) // 1

	var rowsDeleted int
	// DELETE FROM users WHERE id = $1 [1]
	err = user.Delete("WHERE id = $1", id).Execute(&rowsDeleted)
	if err != nil {
		panic(err)
	}
	fmt.Println(rowsDeleted) // 1
}
```

## db.DB

Interface for you to switch between
[github.com/lib/pq](https://github.com/lib/pq) and
[github.com/jackc/pgx](https://github.com/jackc/pgx) PostgreSQL driver.

```go
package main

import (
	"flag"
	"log"

	"github.com/caiguanhao/furk/db"
	"github.com/caiguanhao/furk/db/pgx"
	"github.com/caiguanhao/furk/db/pq"
)

func openDB(usePgxPool bool, conn string) (db.DB, error) {
	if usePgxPool {
		return pgx.Open(conn)
	}
	return pq.Open(conn)
}

func main() {
	usePgxpool := flag.Bool("pgxpool", false, "use pgxpool instead of pq")
	flag.Parse()
	conn, err := openDB(*usePgxpool, "postgres://localhost:5432/yourdb?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
}
```
