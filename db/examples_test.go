package db_test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/caiguanhao/furk/db"
	"github.com/caiguanhao/furk/db/gopg"
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

func ExampleGOPG() {
	conn := gopg.MustOpen("postgres://localhost:5432/furktests?sslmode=disable")
	defer conn.Close()

	var output0 string
	output1 := map[int]string{}
	var output2 []int

	m := db.NewModelTable("", conn, logger.StandardLogger)
	m.NewSQLWithValues("SELECT current_database()").MustQueryRowInTransaction(&db.TxOptions{
		Before: func(ctx context.Context, tx db.Tx) (err error) {
			// just like tx.ExecContext(ctx, ...)
			err = m.NewSQLWithValues("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").ExecTx(tx, ctx)
			if err != nil {
				return
			}
			var rows db.Rows
			// just like tx.QueryContext(ctx, ...)
			rows, err = m.NewSQLWithValues("SELECT s.a, chr(s.a) FROM generate_series(65,70) AS s(a)").QueryTx(tx, ctx)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var key int
				var value string
				if err = rows.Scan(&key, &value); err != nil {
					return
				}
				output1[key] = value
			}
			err = rows.Err()
			return
		},
		After: func(ctx context.Context, tx db.Tx) error {
			// just like tx.QueryContext(ctx, ...)
			rows, err := m.NewSQLWithValues("SELECT * FROM generate_series(11, 15)").QueryTx(tx, ctx)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var id int
				if err := rows.Scan(&id); err != nil {
					return err
				}
				output2 = append(output2, id)
			}
			return rows.Err()
		},
	}, &output0)
	fmt.Println("output0:", output0)
	fmt.Println("output1:", output1)
	fmt.Println("output2:", output2)

	var outpu3 map[int]string
	m.NewSQLWithValues("SELECT s.a, chr(s.a) FROM generate_series(71,75) AS s(a)").MustQuery(&outpu3)
	fmt.Println("output3:", outpu3)

	// Output:
	// output0: furktests
	// output1: map[65:A 66:B 67:C 68:D 69:E 70:F]
	// output2: [11 12 13 14 15]
	// output3: map[71:G 72:H 73:I 74:J 75:K]
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
