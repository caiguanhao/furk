package db

import (
	"context"
)

type (
	SQLDB interface {
		Close() error
		Exec(query string, args ...interface{}) (SQLResult, error)
		Query(query string, args ...interface{}) (SQLRows, error)
		QueryRow(query string, args ...interface{}) SQLRow
		BeginTx(ctx context.Context, isolationLevel string) (SQLTx, error)
		ErrNoRows() error
		ErrHasCode(err error, code string) bool
	}

	SQLTx interface {
		ExecContext(ctx context.Context, query string, args ...interface{}) (SQLResult, error)
		QueryContext(ctx context.Context, query string, args ...interface{}) (SQLRows, error)
		QueryRowContext(ctx context.Context, query string, args ...interface{}) SQLRow
		Commit(ctx context.Context) error
		Rollback(ctx context.Context) error
	}

	SQLResult interface {
		RowsAffected() (int64, error)
	}

	SQLRows interface {
		Close() error
		Err() error
		Next() bool
		Scan(dest ...interface{}) error
	}

	SQLRow interface {
		Scan(dest ...interface{}) error
	}
)
