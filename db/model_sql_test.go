package db_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/caiguanhao/furk/db"
	"github.com/caiguanhao/furk/logger"
	_ "github.com/lib/pq"
)

type (
	order struct {
		__TABLE_NAME__ string `orders`

		Id          int
		Status      string
		TradeNumber string
		UserId      int `json:"foobar_user_id"`
		CreatedAt   time.Time
		UpdatedAt   time.Time
		name        string `column:"name"`
		title       string `column:"title,options"`
		Ignored     string `column:"-"`
		ignored     string

		FieldInJsonb string `jsonb:"meta"`
		OtherJsonb   string `json:"otherjsonb" jsonb:"meta"`
		jsonbTest    int    `json:"testjsonb" column:"JSONBTEST" jsonb:"meta"`
	}
)

func TestCRUD(t *testing.T) {
	c, err := sql.Open("postgres", "postgres://localhost:5432/flurktests?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Ping(); err != nil {
		t.Fatal(err)
	}
	conn := &db.StandardDB{c}

	o := db.NewModel(order{})
	o.SetConnection(conn)
	o.SetLogger(logger.StandardLogger)
	err = o.NewSQLWithValues("DROP TABLE IF EXISTS " + o.TableName()).Execute()
	if err != nil {
		t.Fatal(err)
	}
	err = o.NewSQLWithValues(o.Schema()).Execute()
	if err != nil {
		t.Fatal(err)
	}

	randomBytes := make([]byte, 10)
	if _, err := rand.Read(randomBytes); err != nil {
		t.Fatal(err)
	}
	tradeNo := hex.EncodeToString(randomBytes)
	createInput := strings.NewReader(`{
		"Status": "changed",
		"TradeNumber": "` + tradeNo + `",
		"foobar_user_id": 1,
		"NotAllowed": "foo",
		"FieldInJsonb": "yes",
		"otherjsonb": "no",
		"testjsonb": 123
	}`)
	var createData map[string]interface{}
	if err := json.NewDecoder(createInput).Decode(&createData); err != nil {
		t.Fatal(err)
	}
	model := db.NewModel(order{})
	model.SetConnection(conn)
	model.SetLogger(logger.StandardLogger)

	var id int
	err = model.Insert(
		model.Permit(
			"Status", "TradeNumber", "UserId", "FieldInJsonb", "OtherJsonb",
			"jsonbTest",
		).Filter(createData),
		model.Changes(db.RawChanges{
			"name":   "foobar",
			"title":  "hello",
			"Status": "new",
		}),
		model.CreatedAt(),
		model.UpdatedAt(),
	)("RETURNING id").QueryRow(&id)
	if err != nil {
		t.Fatal(err)
	}
	testI(t, "first order id", id, 1)

	err = model.Insert(
		model.Changes(db.RawChanges{
			"Status": "new",
		}),
	)("RETURNING id").QueryRow(&id)
	if err != nil {
		t.Fatal(err)
	}
	testI(t, "second order id", id, 2)

	var firstOrder order
	err = model.Find("ORDER BY created_at ASC").Query(&firstOrder)
	if err != nil {
		t.Fatal(err)
	}
	testI(t, "order id", firstOrder.Id, 1)
	testS(t, "order status", firstOrder.Status, "new")
	testS(t, "order trade number", firstOrder.TradeNumber, tradeNo)
	testI(t, "order user", firstOrder.UserId, 1)
	testS(t, "order name", firstOrder.name, "foobar")
	testS(t, "order title", firstOrder.title, "hello")
	ca := time.Since(firstOrder.CreatedAt)
	ua := time.Since(firstOrder.UpdatedAt)
	fmt.Println(ca, ua)
	testB(t, "order created at", ca > 0 && ca < 200*time.Millisecond)
	testB(t, "order updated at", ua > 0 && ua < 200*time.Millisecond)
	testS(t, "order ignored", firstOrder.Ignored, "")
	testS(t, "order ignored #2", firstOrder.ignored, "")
	testS(t, "order FieldInJsonb", firstOrder.FieldInJsonb, "yes")
	testS(t, "order OtherJsonb", firstOrder.OtherJsonb, "no")
	testI(t, "order jsonbTest", firstOrder.jsonbTest, 123)

	var orders []order
	err = model.Find("ORDER BY created_at DESC").Query(&orders)
	if err != nil {
		t.Fatal(err)
	}
	testI(t, "orders size", len(orders), 2)
	testI(t, "first order id", orders[0].Id, 2)
	testI(t, "first order jsonbTest", orders[0].jsonbTest, 0)
	testI(t, "second order id", orders[1].Id, 1)
	testI(t, "second order jsonbTest", orders[1].jsonbTest, 123)

	time.Sleep(200 * time.Millisecond)
	updateInput := strings.NewReader(`{
		"Status": "modified",
		"NotAllowed": "foo",
		"FieldInJsonb": "red",
		"otherjsonb": "blue"
	}`)
	var updateData map[string]interface{}
	err = json.NewDecoder(updateInput).Decode(&updateData)
	if err != nil {
		t.Fatal(err)
	}
	var rowsAffected int
	err = model.Update(
		model.Permit("Status", "FieldInJsonb", "OtherJsonb").Filter(updateData),
		model.Permit("Status").Filter(db.RawChanges{
			"x":            "1",
			"Status":       "furk",
			"FieldInJsonb": "black",
		}),
		model.UpdatedAt(),
	)().ExecuteInTransaction(&db.TxOptions{
		IsolationLevel: db.LevelSerializable,
		Before: func(ctx context.Context, tx db.Tx) (err error) {
			err = model.NewSQLWithValues(
				"UPDATE "+model.TableName()+" SET user_id = user_id - $1",
				23,
			).ExecTx(tx, ctx)
			return
		},
		After: func(ctx context.Context, tx db.Tx) (err error) {
			err = model.NewSQLWithValues(
				"UPDATE "+model.TableName()+" SET user_id = user_id + $1",
				99,
			).ExecTx(tx, ctx)
			return
		},
	}, &rowsAffected)
	if err != nil {
		t.Fatal(err)
	}
	testI(t, "rows affected", rowsAffected, 2)

	var secondOrder order
	err = model.Find("WHERE id = $1", 2).Query(&secondOrder)
	if err != nil {
		t.Fatal(err)
	}
	testI(t, "order id", secondOrder.Id, 2)
	testS(t, "order status", secondOrder.Status, "furk")
	ca = time.Since(secondOrder.CreatedAt)
	ua = time.Since(secondOrder.UpdatedAt)
	testB(t, "order created at", ca > 200*time.Millisecond) // because of time.Sleep
	testB(t, "order updated at", ua > 0 && ua < 200*time.Millisecond)
	testS(t, "order FieldInJsonb", secondOrder.FieldInJsonb, "red")
	testS(t, "order OtherJsonb", secondOrder.OtherJsonb, "blue")
	var u int
	testI(t, "order user", secondOrder.UserId, u-23+99)

	count, err := model.Count()
	if err != nil {
		t.Fatal(err)
	}
	testI(t, "rows count", count, 2)

	var rowsDeleted int
	err = model.Delete().Execute(&rowsDeleted)
	if err != nil {
		t.Fatal(err)
	}
	testI(t, "rows deleted", rowsDeleted, 2)

	count, err = model.Count()
	if err != nil {
		t.Fatal(err)
	}
	testI(t, "rows count", count, 0)
}

func testB(t *testing.T, name string, b bool) {
	t.Helper()
	if b {
		t.Logf("%s test passed", name)
	} else {
		t.Errorf("%s test failed, got %t", name, b)
	}
}

func testS(t *testing.T, name, got, expected string) {
	t.Helper()
	if got == expected {
		t.Logf("%s test passed", name)
	} else {
		t.Errorf("%s test failed, got %s", name, got)
	}
}

func testI(t *testing.T, name string, got, expected int) {
	t.Helper()
	if got == expected {
		t.Logf("%s test passed", name)
	} else {
		t.Errorf("%s test failed, got %d", name, got)
	}
}
