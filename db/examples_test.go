package db_test

import (
	"database/sql"
	"fmt"

	"github.com/caiguanhao/furk/db"
	"github.com/caiguanhao/furk/db/pgx"
	"github.com/caiguanhao/furk/db/pq"
	"github.com/caiguanhao/furk/db/standard"
	"github.com/caiguanhao/furk/logger"

	_ "github.com/lib/pq"
)

type (
	Post struct {
		Id      int
		Title   string
		Picture string `jsonb:"meta"`
	}
)

func ExamplePQ() {
	c, err := sql.Open("postgres", "postgres://localhost:5432/furktests?sslmode=disable")
	if err != nil {
		panic(err)
	}
	var conn db.DB = &standard.DB{c}
	defer conn.Close()
	if err := c.Ping(); err != nil {
		panic(err)
	}
	var name string
	db.NewModelTable("", conn).NewSQLWithValues("SELECT current_database()").MustQueryRow(&name)
	fmt.Println(name)
	// Output:
	// furktests
}

func ExamplePost() {
	connStr := "postgres://localhost:5432/furktests?sslmode=disable"
	usePgxPool := true
	var conn db.DB
	if usePgxPool {
		conn = pgx.MustOpen(connStr)
	} else {
		conn = pq.MustOpen(connStr)
	}
	defer conn.Close()

	m := db.NewModel(Post{}, conn, logger.StandardLogger)

	fmt.Println(m.Schema())
	m.NewSQLWithValues(m.Schema()).MustExecute()

	defer func() {
		fmt.Println(m.DropSchema())
		m.NewSQLWithValues(m.DropSchema()).MustExecute()
	}()

	var newPostId int
	i := m.Insert(
		m.Permit("Title", "Picture").Filter(`{ "Title": "hello", "Picture": "world!" }`),
	)("RETURNING id")
	fmt.Println(i)
	i.MustQueryRow(&newPostId)
	fmt.Println("id:", newPostId)

	var firstPost Post
	m.Find("WHERE id = $1", newPostId).MustQuery(&firstPost)
	fmt.Println("post:", firstPost)

	var ids []int
	m.Select("id", "ORDER BY id ASC").MustQuery(&ids)
	fmt.Println("ids:", ids)

	var id2title map[int]string
	m.Select("id, title").MustQuery(&id2title)
	fmt.Println("map:", id2title)

	var rowsAffected int
	u := m.Update(
		m.Permit("Picture").Filter(`{ "Picture": "WORLD!" }`),
	)("WHERE id = $1", newPostId)
	fmt.Println(u)
	u.MustExecute(&rowsAffected)
	fmt.Println("updated:", rowsAffected)

	var posts []Post
	m.Find().MustQuery(&posts)
	fmt.Println("posts:", posts)

	e := m.MustExists("WHERE id = $1", newPostId)
	fmt.Println("exists:", e)

	c := m.MustCount()
	fmt.Println("count:", c)

	var rowsDeleted int
	d := m.Delete("WHERE id = $1", newPostId)
	fmt.Println(d)
	d.MustExecute(&rowsDeleted)
	fmt.Println("deleted:", rowsDeleted)

	// Output:
	// CREATE TABLE posts (
	// 	id SERIAL PRIMARY KEY,
	// 	title text DEFAULT ''::text NOT NULL,
	// 	meta jsonb DEFAULT '{}'::jsonb NOT NULL
	// );
	//
	// INSERT INTO posts (title, meta) VALUES ($1, $2) RETURNING id
	// id: 1
	// post: {1 hello world!}
	// ids: [1]
	// map: map[1:hello]
	// UPDATE posts SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', $2) WHERE id = $1
	// updated: 1
	// posts: [{1 hello WORLD!}]
	// exists: true
	// count: 1
	// DELETE FROM posts WHERE id = $1
	// deleted: 1
	// DROP TABLE IF EXISTS posts;
}
