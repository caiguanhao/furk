package db

import (
	"context"
	"database/sql"
)

type (
	StandardDB struct {
		*sql.DB
	}

	StandardTx struct {
		*sql.Tx
	}
)

func (d *StandardDB) Close() error {
	return d.DB.Close()
}

func (d *StandardDB) Exec(query string, args ...interface{}) (Result, error) {
	return d.DB.Exec(query, args...)
}

func (d *StandardDB) Query(query string, args ...interface{}) (Rows, error) {
	return d.DB.Query(query, args...)
}

func (d *StandardDB) QueryRow(query string, args ...interface{}) Row {
	return d.DB.QueryRow(query, args...)
}

func (d *StandardDB) BeginTx(ctx context.Context, isolationLevel string) (Tx, error) {
	var isolation sql.IsolationLevel
	switch isolationLevel {
	case "serializable":
		isolation = sql.LevelSerializable
	case "repeatable read":
		isolation = sql.LevelRepeatableRead
	case "read committed":
		isolation = sql.LevelReadCommitted
	case "read uncommitted":
		isolation = sql.LevelReadUncommitted
	}
	tx, err := d.DB.BeginTx(ctx, &sql.TxOptions{
		Isolation: isolation,
	})
	return &StandardTx{tx}, err
}

func (d *StandardDB) ErrNoRows() error {
	return sql.ErrNoRows
}

func (d *StandardDB) ErrGetCode(err error) string {
	if e, ok := err.(interface{ Get(byte) string }); ok { // github.com/lib/pq
		return e.Get('C')
	}
	return "unknown"
}

func (t *StandardTx) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return t.Tx.ExecContext(ctx, query, args...)
}

func (t *StandardTx) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return t.Tx.QueryContext(ctx, query, args...)
}

func (t *StandardTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) Row {
	return t.Tx.QueryRowContext(ctx, query, args...)
}

func (t *StandardTx) Commit(ctx context.Context) error {
	return t.Tx.Commit()
}

func (t *StandardTx) Rollback(ctx context.Context) error {
	return t.Tx.Rollback()
}
