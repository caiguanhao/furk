package db

import (
	"testing"
	"time"
)

type (
	admin struct {
		Id       int
		Name     string
		Password string
	}

	category struct {
		Id        int
		Names     []map[string]string `jsonb:"meta"`
		Picture   string              `jsonb:"meta"`
		CreatedAt time.Time
		UpdatedAt time.Time
	}
)

func TestModel(t *testing.T) {
	var i int
	testS := func(got, expected string) {
		t.Helper()
		if got == expected {
			t.Logf("case %d passed", i)
		} else {
			t.Errorf("case %d failed, got %s", i, got)
		}
		i++
	}
	testI := func(got, expected int) {
		t.Helper()
		if got == expected {
			t.Logf("case %d passed", i)
		} else {
			t.Errorf("case %d failed, got %d", i, got)
		}
		i++
	}

	m0 := NewModelSlim(admin{})
	testS(m0.tableName, "admins")
	testI(len(m0.modelFields), 0)
	m0.Permit("Id")
	testI(len(m0.PermittedFields()), 0)

	m1 := NewModel(admin{})
	testS(m1.tableName, "admins")
	testI(len(m1.modelFields), 3)
	testI(len(m1.PermittedFields()), 0)
	m1.Permit("Invalid")
	testI(len(m1.PermittedFields()), 0)
	m1.Permit("Id")
	testI(len(m1.PermittedFields()), 1)
	m1.Permit("Id", "Id")
	testI(len(m1.PermittedFields()), 1)
	testI(len(m1.Filter(map[string]interface{}{
		"Id":   1,
		"Name": "haha",
	})), 1)
	m1.Permit()
	testI(len(m1.PermittedFields()), 0)
	testI(len(m1.Filter(map[string]interface{}{
		"Name": "haha",
	})), 0)
	m1.PermitAllExcept("Password")
	testI(len(m1.PermittedFields()), 2)
	m1.PermitAllExcept("Password", "Password")
	testI(len(m1.PermittedFields()), 2)
	testI(len(m1.Filter(map[string]interface{}{
		"Name":     "haha",
		"Password": "reset",
		"BadData":  "foobar",
	})), 1)
	m1.PermitAllExcept()
	testI(len(m1.PermittedFields()), 3)
	m1.PermitAllExcept("Invalid")
	testI(len(m1.PermittedFields()), 3)
	m1.Permit()
	c := m1.Changes(RawChanges{
		"Name":    "test",
		"BadData": "foobar",
	})
	testI(len(c), 1)
	var f field
	for _f := range c {
		f = _f
		break
	}
	testS(f.Name, "Name")
	testS(m1.Find().String(), "SELECT id, name, password FROM admins")
	testS(m1.Delete().String(), "DELETE FROM admins")
	testS(m1.Delete("WHERE id = $1", 1).String(),
		"DELETE FROM admins WHERE id = $1")
	testS(m1.Insert(c)().String(), "INSERT INTO admins (name) VALUES ($1)")
	testS(m1.Update(c)().String(), "UPDATE admins SET name = $1")
	testS(m1.Update(c)("WHERE id = $1", 1).String(),
		"UPDATE admins SET name = $2 WHERE id = $1")

	m2 := NewModel(category{})
	testS(m2.tableName, "categories")
	m2.Permit("Names", "Picture")
	testI(len(m2.PermittedFields()), 2)
	m2c := m2.Changes(RawChanges{
		"Picture": "https://hello/world",
	})
	testS(m2.Insert(m2c)().String(), "INSERT INTO categories (meta) VALUES ($1)")
	testS(m2.Insert(m2c)().values[0].(string), `{"picture":"https://hello/world"}`)
	testS(m2.Update(m2c)().String(), "UPDATE categories SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', $1)")
	testS(m2.Update(m2c)().values[0].(string), `"https://hello/world"`)
	testS(m2.Update(m2c)("WHERE id = $1", 1).String(),
		"UPDATE categories SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{picture}', $2) WHERE id = $1")
	m2c2 := m2.Changes(RawChanges{
		"Names": []map[string]string{
			{
				"key":   "en_US",
				"value": "Category",
			},
		},
	})
	testS(m2.Insert(m2c2)().String(), "INSERT INTO categories (meta) VALUES ($1)")
	testS(m2.Insert(m2c2)().values[0].(string), `{"names":[{"key":"en_US","value":"Category"}]}`)
	testS(m2.Update(m2c2)().String(), "UPDATE categories SET meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{names}', $1)")
	testS(m2.Update(m2c2)().values[0].(string), `[{"key":"en_US","value":"Category"}]`)
	testS(m2.Insert(
		m2c2,
		m2.CreatedAt(),
		m2.UpdatedAt(),
	)().String(), "INSERT INTO categories (created_at, updated_at, meta) VALUES ($1, $2, $3)")
	testS(m2.Update(
		m2c2,
		m2.CreatedAt(),
		m2.UpdatedAt(),
	)().String(), "UPDATE categories SET created_at = $1, updated_at = $2, meta = jsonb_set(COALESCE(meta, '{}'::jsonb), '{names}', $3)")
}
