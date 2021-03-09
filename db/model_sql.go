package db

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"unsafe"

	"github.com/caiguanhao/furk/logger"
)

const (
	actionQueryRow = iota
	actionExecute
)

var (
	ErrInvalidTarget       = errors.New("target must be pointer of a struct or pointer of a slice of structs")
	ErrNoConnection        = errors.New("no connection")
	ErrTypeAssertionFailed = errors.New("type assertion failed")
)

type (
	TxOptions struct {
		IsolationLevel string
		Before, After  func(context.Context, Tx) error
	}

	sqlWithValues struct {
		model  *Model
		sql    string
		values []interface{}
	}

	jsonbRaw map[string]json.RawMessage
)

func (j *jsonbRaw) Scan(src interface{}) error { // necessary for github.com/lib/pq
	if src == nil {
		return nil
	}
	source, ok := src.([]byte)
	if !ok {
		return ErrTypeAssertionFailed
	}
	return json.Unmarshal(source, j)
}

func (m Model) NewSQLWithValues(sql string, values ...interface{}) sqlWithValues {
	sql = strings.TrimSpace(sql)
	return sqlWithValues{
		model:  &m,
		sql:    sql,
		values: values,
	}
}

func (s sqlWithValues) String() string {
	return s.sql
}

func (s sqlWithValues) MustQuery(target interface{}) {
	if err := s.Query(target); err != nil {
		panic(err)
	}
}

// get one (if target is a pointer of struct) or all results (if target is a
// pointer of a slice of struct) from database
func (s sqlWithValues) Query(target interface{}) error {
	if s.model.connection == nil {
		return ErrNoConnection
	}

	rt := reflect.TypeOf(target)
	if rt.Kind() != reflect.Ptr {
		return ErrInvalidTarget
	}
	rt = rt.Elem()

	kind := rt.Kind()
	if kind == reflect.Struct { // if target is not a slice, use QueryRow instead
		rv := reflect.Indirect(reflect.ValueOf(target))
		s.log(s.sql, s.values)
		return s.scan(rv, s.model.connection.QueryRow(s.sql, s.values...))
	} else if kind != reflect.Slice {
		return ErrInvalidTarget
	}

	rt = rt.Elem()
	s.log(s.sql, s.values)
	rows, err := s.model.connection.Query(s.sql, s.values...)
	if err != nil {
		return err
	}
	defer rows.Close()
	v := reflect.Indirect(reflect.ValueOf(target))
	for rows.Next() {
		rv := reflect.New(rt).Elem()
		if err := s.scan(rv, rows); err != nil {
			return err
		}
		v.Set(reflect.Append(v, rv))
	}
	return rows.Err()
}

// scan a scannable (Row or Rows) into every field of a struct
func (s sqlWithValues) scan(rv reflect.Value, scannable Scannable) error {
	if rv.Kind() != reflect.Struct || rv.Type() != s.model.structType {
		return scannable.Scan(rv.Addr().Interface())
	}
	f := rv.FieldByName(tableNameField)
	if f.Kind() == reflect.String {
		// hack
		reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().SetString(s.model.tableName)
	}
	dests := []interface{}{}
	for _, field := range s.model.modelFields {
		if field.Jsonb != "" {
			continue
		}
		f := rv.FieldByName(field.Name)
		if field.Exported {
			pointer := f.Addr().Interface()
			dests = append(dests, pointer)
		} else {
			pointer := reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Interface()
			dests = append(dests, pointer)
		}
	}
	jsonbValues := []jsonbRaw{}
	for range s.model.jsonbColumns {
		jsonb := jsonbRaw{}
		dests = append(dests, &jsonb)
		jsonbValues = append(jsonbValues, jsonb)
	}
	if err := scannable.Scan(dests...); err != nil {
		return err
	}
	for _, jsonb := range jsonbValues {
		for _, field := range s.model.modelFields {
			if field.Jsonb == "" {
				continue
			}
			val, ok := jsonb[field.ColumnName]
			if !ok {
				continue
			}
			f := rv.FieldByName(field.Name)
			var pointer interface{}
			if field.Exported {
				pointer = f.Addr().Interface()
			} else {
				pointer = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Interface()
			}
			if err := json.Unmarshal(val, pointer); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s sqlWithValues) MustQueryRow(dest ...interface{}) {
	if err := s.QueryRow(dest...); err != nil {
		panic(err)
	}
}

// get returning results from an INSERT INTO statement
func (s sqlWithValues) QueryRow(dest ...interface{}) error {
	return s.QueryRowInTransaction(nil, dest...)
}

func (s sqlWithValues) MustQueryRowInTransaction(txOpts *TxOptions, dest ...interface{}) {
	if err := s.QueryRowInTransaction(txOpts, dest...); err != nil {
		panic(err)
	}
}

func (s sqlWithValues) QueryRowInTransaction(txOpts *TxOptions, dest ...interface{}) error {
	return s.execute(actionQueryRow, txOpts, dest...)
}

func (s sqlWithValues) MustExecute(dest ...interface{}) {
	if err := s.Execute(dest...); err != nil {
		panic(err)
	}
}

// execute statements like INSERT INTO, UPDATE, DELETE and get rows affected
func (s sqlWithValues) Execute(dest ...interface{}) error {
	return s.ExecuteInTransaction(nil, dest...)
}

func (s sqlWithValues) MustExecuteInTransaction(txOpts *TxOptions, dest ...interface{}) {
	if err := s.ExecuteInTransaction(txOpts, dest...); err != nil {
		panic(err)
	}
}

func (s sqlWithValues) ExecuteInTransaction(txOpts *TxOptions, dest ...interface{}) error {
	return s.execute(actionExecute, txOpts, dest...)
}

// execute a transaction
func (s sqlWithValues) ExecTx(tx Tx, ctx context.Context, dest ...interface{}) (err error) {
	if s.model.connection == nil {
		err = ErrNoConnection
		return
	}
	s.log(s.sql, s.values)
	err = returnRowsAffected(dest)(tx.ExecContext(ctx, s.sql, s.values...))
	return
}

func (s sqlWithValues) execute(action int, txOpts *TxOptions, dest ...interface{}) (err error) {
	if s.model.connection == nil {
		err = ErrNoConnection
		return
	}
	if txOpts == nil || (txOpts.Before == nil && txOpts.After == nil) {
		s.log(s.sql, s.values)
		if action == actionQueryRow {
			err = s.model.connection.QueryRow(s.sql, s.values...).Scan(dest...)
			return
		}
		err = returnRowsAffected(dest)(s.model.connection.Exec(s.sql, s.values...))
		return
	}
	ctx := context.Background()
	s.log("BEGIN", nil)
	tx, err := s.model.connection.BeginTx(ctx, txOpts.IsolationLevel)
	if err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			s.log("ROLLBACK", nil)
			tx.Rollback(ctx)
		} else if err != nil {
			s.log("ROLLBACK", nil)
			tx.Rollback(ctx)
		} else {
			s.log("COMMIT", nil)
			err = tx.Commit(ctx)
		}
	}()
	if txOpts.Before != nil {
		err = txOpts.Before(ctx, tx)
		if err != nil {
			return
		}
	}
	s.log(s.sql, s.values)
	if action == actionQueryRow {
		err = tx.QueryRowContext(ctx, s.sql, s.values...).Scan(dest...)
	} else {
		err = returnRowsAffected(dest)(tx.ExecContext(ctx, s.sql, s.values...))
	}
	if err != nil {
		return
	}
	if txOpts.After != nil {
		err = txOpts.After(ctx, tx)
		if err != nil {
			return
		}
	}
	return
}

func (s sqlWithValues) log(sql string, args []interface{}) {
	if s.model.logger == nil {
		return
	}
	var prefix string
	if idx := strings.Index(sql, " "); idx > -1 {
		prefix = strings.ToUpper(sql[:idx])
	} else {
		prefix = strings.ToUpper(sql)
	}
	var colored logger.ColoredString
	switch prefix {
	case "DELETE", "DROP", "ROLLBACK":
		colored = logger.RedString(sql)
	case "INSERT", "CREATE", "COMMIT":
		colored = logger.GreenString(sql)
	case "UPDATE", "ALTER":
		colored = logger.YellowString(sql)
	default:
		colored = logger.CyanString(sql)
	}
	if len(args) == 0 {
		s.model.logger.Debug(colored)
		return
	}
	s.model.logger.Debug(colored, args)
}

func returnRowsAffected(dest []interface{}) func(Result, error) error {
	return func(result Result, err error) error {
		if err != nil {
			return err
		}
		if len(dest) == 0 {
			return nil
		}
		ra, err := result.RowsAffected()
		if err != nil {
			return err
		}
		switch x := dest[0].(type) {
		case *int:
			*x = int(ra)
		case *int64:
			*x = ra
		}
		return nil
	}
}
