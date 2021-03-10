package db_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/caiguanhao/furk/db"
	"github.com/caiguanhao/furk/db/pgx"
	"github.com/caiguanhao/furk/db/pq"
	"github.com/caiguanhao/furk/logger"
	"github.com/shopspring/decimal"
)

type (
	order struct {
		__TABLE_NAME__ string `orders`

		Id          int
		Status      string
		TradeNumber string
		UserId      int `json:"foobar_user_id"`
		TotalAmount decimal.Decimal
		CreatedAt   time.Time
		UpdatedAt   time.Time
		name        string `column:"name"`
		title       string `column:"title,options"`
		Ignored     string `column:"-"`
		ignored     string

		FieldInJsonb string `jsonb:"meta"`
		OtherJsonb   string `json:"otherjsonb" jsonb:"meta"`
		jsonbTest    int    `json:"testjsonb" column:"JSONBTEST" jsonb:"meta"`
		BadType      int    `jsonb:"meta"`
		Sources      []struct {
			Name string
		} `jsonb:"meta"`
		Sources2 map[string]int `jsonb:"meta2"`
		Sources3 struct {
			Word string
		} `jsonb:"meta3"`
	}
)

var connStr string

func init() {
	connStr = os.Getenv("DBCONNSTR")
	if connStr == "" {
		connStr = "postgres://localhost:5432/flurktests?sslmode=disable"
	}
}

func TestCRUDInPQ(t *testing.T) {
	conn, err := pq.Open(connStr)
	if err != nil {
		t.Fatal(err)
	}
	testCRUD(t, conn)
}

func TestCRUDInPGX(t *testing.T) {
	conn, err := pgx.Open(connStr)
	if err != nil {
		t.Fatal(err)
	}
	testCRUD(t, conn)
}

func testCRUD(t *testing.T, conn db.DB) {
	o := db.NewModel(order{})
	o.SetConnection(conn)
	o.SetLogger(logger.StandardLogger)

	// drop table
	err := o.NewSQLWithValues(o.DropSchema()).Execute()
	if err != nil {
		t.Fatal(err)
	}

	// create table
	err = o.NewSQLWithValues(o.Schema()).Execute()
	if err != nil {
		t.Fatal(err)
	}

	randomBytes := make([]byte, 10)
	if _, err := rand.Read(randomBytes); err != nil {
		t.Fatal(err)
	}
	tradeNo := hex.EncodeToString(randomBytes)
	totalAmount, _ := decimal.NewFromString("12.34")
	createInput := strings.NewReader(`{
		"Status": "changed",
		"TradeNumber": "` + tradeNo + `",
		"TotalAmount": "` + totalAmount.String() + `",
		"foobar_user_id": 1,
		"NotAllowed": "foo",
		"FieldInJsonb": "yes",
		"otherjsonb": "no",
		"testjsonb": 123,
		"BadType": "string",
		"Sources": [{
			"Name": "yes",
			"baddata": "foobar"
		}],
		"Sources2": {
			"cash": 100
		},
		"Sources3": {
			"Word": "finish"
		}
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
			"jsonbTest", "TotalAmount", "BadType", "Sources", "Sources2", "Sources3",
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

	var badType, sources, sources2, sources3 string
	model.Select(
		"COALESCE(meta->>'bad_type', 'empty'), meta->>'sources', meta2::text, meta3::text",
	).MustQueryRow(&badType, &sources, &sources2, &sources3)
	// field with wrong type is skipped, so empty is returned
	testS(t, "first order bad type", badType, "empty")
	// unwanted content "baddata" is filtered
	testS(t, "first order sources", sources, `[{"Name": "yes"}]`)
	testS(t, "first order sources 2", sources2, `{"sources2": {"cash": 100}}`)      // map
	testS(t, "first order sources 3", sources3, `{"sources3": {"Word": "finish"}}`) // struct

	exists := model.MustExists("WHERE id = $1", id)
	testB(t, "first order exists", exists)

	exists2 := model.MustExists("WHERE id = $1", id+1)
	testB(t, "first order exists #2", exists2 == false)

	err = model.Insert(
		model.Changes(db.RawChanges{
			"Status": "new2",
		}),
	)("RETURNING id").QueryRow(&id)
	if err != nil {
		t.Fatal(err)
	}
	testI(t, "second order id", id, 2)

	var statuses []string
	model.Select("status").MustQuery(&statuses)
	testI(t, "statuses length", len(statuses), 2)
	testS(t, "status 0", statuses[0], "new")
	testS(t, "status 1", statuses[1], "new2")
	var ids []int
	model.Select("id").MustQuery(&ids)
	testI(t, "ids length", len(ids), 2)
	testI(t, "id 0", ids[0], 1)
	testI(t, "id 1", ids[1], 2)
	id2status := map[int]string{}
	model.Select("id, status").MustQuery(&id2status)
	testI(t, "map length", len(id2status), 2)
	testS(t, "map 0", id2status[1], "new")
	testS(t, "map 1", id2status[2], "new2")
	var status2id map[string]int
	model.Select("status, id").MustQuery(&status2id)
	testI(t, "map length", len(status2id), 2)
	testI(t, "map 0", status2id["new"], 1)
	testI(t, "map 1", status2id["new2"], 2)
	var createdAts []time.Time
	model.Select("created_at").MustQuery(&createdAts)
	testI(t, "created_at length", len(createdAts), 2)
	d1 := time.Since(createdAts[0])
	d2 := time.Since(createdAts[1])
	testB(t, "created_at 0", d1 > 0 && d1 < 200*time.Millisecond)
	testB(t, "created_at 1", d2 > 0 && d2 < 200*time.Millisecond)

	var firstOrder order
	err = model.Find("ORDER BY created_at ASC").Query(&firstOrder)
	if err != nil {
		t.Fatal(err)
	}
	testI(t, "order id", firstOrder.Id, 1)
	testS(t, "order status", firstOrder.Status, "new")
	testS(t, "order trade number", firstOrder.TradeNumber, tradeNo)
	testD(t, "order total amount", firstOrder.TotalAmount, totalAmount)
	testI(t, "order user", firstOrder.UserId, 1)
	testS(t, "order name", firstOrder.name, "foobar")
	testS(t, "order title", firstOrder.title, "hello")
	ca := time.Since(firstOrder.CreatedAt)
	ua := time.Since(firstOrder.UpdatedAt)
	testB(t, "order created at", ca > 0 && ca < 200*time.Millisecond)
	testB(t, "order updated at", ua > 0 && ua < 200*time.Millisecond)
	testS(t, "order ignored", firstOrder.Ignored, "")
	testS(t, "order ignored #2", firstOrder.ignored, "")
	testS(t, "order FieldInJsonb", firstOrder.FieldInJsonb, "yes")
	testS(t, "order OtherJsonb", firstOrder.OtherJsonb, "no")
	testI(t, "order jsonbTest", firstOrder.jsonbTest, 123)

	var c echoContext
	changes, err := model.Permit().Bind(c, &firstOrder)
	if err != nil {
		t.Fatal(err)
	}
	testI(t, "bind changes size", len(changes), 0)
	testI(t, "bind order id", firstOrder.Id, 1)
	testS(t, "bind order status", firstOrder.Status, "new")
	testS(t, "bind order trade number", firstOrder.TradeNumber, tradeNo)
	changes, err = model.Permit("Id", "TradeNumber").Bind(c, &firstOrder)
	if err != nil {
		t.Fatal(err)
	}
	testI(t, "bind changes size", len(changes), 2)
	testI(t, "bind order id", firstOrder.Id, 2)
	testS(t, "bind order status", firstOrder.Status, "new")
	testS(t, "bind order trade number", firstOrder.TradeNumber, "")

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
	var ao order
	achanges, err := model.Assign(
		&ao,
		model.Permit("Status", "FieldInJsonb", "OtherJsonb").Filter(updateData),
		model.Permit("Status").Filter(db.RawChanges{
			"x":            "1",
			"Status":       "furk",
			"FieldInJsonb": "black",
		}),
		model.UpdatedAt(),
	)
	if err != nil {
		t.Fatal(err)
	}
	testS(t, "order status", ao.Status, "furk")
	testS(t, "order FieldInJsonb", ao.FieldInJsonb, "red")
	testS(t, "order OtherJsonb", ao.OtherJsonb, "blue")
	var rowsAffected int
	err = model.Update(achanges...)().ExecuteInTransaction(&db.TxOptions{
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

func testD(t *testing.T, name string, got, expected decimal.Decimal) {
	t.Helper()
	if got.Equal(expected) {
		t.Logf("%s test passed", name)
	} else {
		t.Errorf("%s test failed, got %d", name, got)
	}
}

type (
	echoContext struct{}
)

func (c echoContext) Bind(i interface{}) error {
	if o, ok := i.(*order); ok {
		o.Id = 2
		o.Status = "foo"
	}
	return nil
}
