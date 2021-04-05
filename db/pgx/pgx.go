package pgx

import (
	"context"

	"github.com/caiguanhao/furk/db"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type (
	DB struct {
		*pgxpool.Pool
	}

	Tx struct {
		pgx.Tx
	}

	Result struct {
		rowsAffected int64
	}

	Rows struct {
		pgx.Rows
	}
)

// MustOpen is like Open but panics if connect operation fails.
func MustOpen(conn string) db.DB {
	c, err := Open(conn)
	if err != nil {
		panic(err)
	}
	return c
}

// Open creates and establishes one connection to database.
func Open(conn string) (db.DB, error) {
	pool, err := pgxpool.Connect(context.Background(), conn)
	if err != nil {
		return nil, err
	}
	return &DB{pool}, nil
}

func (d *DB) Close() error {
	d.Pool.Close()
	return nil
}

func (d *DB) Exec(query string, args ...interface{}) (db.Result, error) {
	re, err := d.Pool.Exec(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	return Result{
		rowsAffected: re.RowsAffected(),
	}, nil
}

func (d *DB) Query(query string, args ...interface{}) (db.Rows, error) {
	rows, err := d.Pool.Query(context.Background(), query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{rows}, nil
}

func (d *DB) QueryRow(query string, args ...interface{}) db.Row {
	return d.Pool.QueryRow(context.Background(), query, args...)
}

func (d *DB) BeginTx(ctx context.Context, isolationLevel string) (db.Tx, error) {
	tx, err := d.Pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.TxIsoLevel(isolationLevel),
	})
	if err != nil {
		return nil, err
	}
	return &Tx{tx}, nil
}

func (d *DB) ErrNoRows() error {
	return pgx.ErrNoRows
}

func (d *DB) ErrGetCode(err error) string {
	if e, ok := err.(interface{ SQLState() string }); ok { // github.com/jackc/pgconn
		return e.SQLState()
	}
	return "unknown"
}

func (t *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (db.Result, error) {
	re, err := t.Tx.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return Result{
		rowsAffected: re.RowsAffected(),
	}, nil
}

func (t *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (db.Rows, error) {
	rows, err := t.Tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{rows}, nil
}

func (t *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) db.Row {
	return t.Tx.QueryRow(ctx, query, args...)
}

func (t *Tx) Commit(ctx context.Context) error {
	return t.Tx.Commit(ctx)
}

func (t *Tx) Rollback(ctx context.Context) error {
	return t.Tx.Rollback(ctx)
}

func (r Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func (r *Rows) Close() error {
	r.Rows.Close()
	return nil
}
