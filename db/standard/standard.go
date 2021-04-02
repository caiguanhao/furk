package standard

import (
	"context"
	"database/sql"

	"github.com/caiguanhao/furk/db"
)

type (
	DB struct {
		*sql.DB
	}

	Tx struct {
		*sql.Tx
	}
)

func (d *DB) Close() error {
	return d.DB.Close()
}

func (d *DB) Exec(query string, args ...interface{}) (db.Result, error) {
	return d.DB.Exec(query, args...)
}

func (d *DB) Query(query string, args ...interface{}) (db.Rows, error) {
	return d.DB.Query(query, args...)
}

func (d *DB) QueryRow(query string, args ...interface{}) db.Row {
	return d.DB.QueryRow(query, args...)
}

func (d *DB) BeginTx(ctx context.Context, isolationLevel string) (db.Tx, error) {
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
	return &Tx{tx}, err
}

func (d *DB) ErrNoRows() error {
	return sql.ErrNoRows
}

func (d *DB) ErrGetCode(err error) string {
	if e, ok := err.(interface{ Get(byte) string }); ok { // github.com/lib/pq
		return e.Get('C')
	}
	return "unknown"
}

func (t *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (db.Result, error) {
	return t.Tx.ExecContext(ctx, query, args...)
}

func (t *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (db.Rows, error) {
	return t.Tx.QueryContext(ctx, query, args...)
}

func (t *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) db.Row {
	return t.Tx.QueryRowContext(ctx, query, args...)
}

func (t *Tx) Commit(ctx context.Context) error {
	return t.Tx.Commit()
}

func (t *Tx) Rollback(ctx context.Context) error {
	return t.Tx.Rollback()
}
