package db_test

import (
	"testing"

	"github.com/caiguanhao/furk/db"
)

type (
	user    struct{}
	product struct{}
)

func (_ product) TableName() string {
	return "different_products"
}

func TestToTableName(t *testing.T) {
	cases := [][]interface{}{
		{struct{}{}, "error_no_table_name"},
		{user{}, "users"},
		{product{}, "different_products"},
		{
			struct {
				__TABLE_NAME__ string `custom_name`
			}{}, "custom_name",
		},
	}
	for i, c := range cases {
		got := db.ToTableName(c[0])
		expected, ok := c[1].(string)
		if !ok {
			t.Errorf("case %d type conversion failed", i)
		}
		if got == expected {
			t.Logf("case %d passed", i)
		} else {
			t.Errorf("case %d failed, got %s", i, got)
		}
	}
}

func TestToColumnName(t *testing.T) {
	cases := [][]string{
		{"column", "column"},
		{"Column", "column"},
		{"ColumnName", "column_name"},
		{" GoodColumnName ", "good_column_name"},
	}
	for i, c := range cases {
		got := db.ToColumnName(c[0])
		expected := c[1]
		if got == expected {
			t.Logf("case %d passed", i)
		} else {
			t.Errorf("case %d failed, got %s", i, got)
		}
	}
}
