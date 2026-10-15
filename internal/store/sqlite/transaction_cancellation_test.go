package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

func TestTransactionCancellationAfterAutomaticRollback(t *testing.T) {
	s, _ := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err := withTx(ctx, s.db, func(tx *sql.Tx) error {
		calls++
		if _, err := tx.ExecContext(ctx, "INSERT INTO workspaces VALUES('cancelled-write',1,'[]')"); err != nil {
			return err
		}
		cancel()
		// The sole connection becomes available only after database/sql rolls back.
		wait, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		conn, err := s.db.Conn(wait)
		if err != nil {
			return err
		}
		return conn.Close()
	})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrUnavailable) || calls != 1 {
		t.Fatalf("cancelled transaction: err=%v calls=%d", err, calls)
	}
	var count int
	if err := s.db.QueryRowContext(context.Background(), "SELECT count(*) FROM workspaces WHERE workspace_id='cancelled-write'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("cancelled write survived: count=%d err=%v", count, err)
	}
}

func TestTransactionCommitAfterExplicitRollbackIsUnavailable(t *testing.T) {
	s, _ := newStore(t)
	calls := 0
	err := withTx(context.Background(), s.db, func(tx *sql.Tx) error {
		calls++
		return tx.Rollback()
	})
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("uncancelled completed transaction: err=%v calls=%d", err, calls)
	}
}

func TestTransactionCallbackFailureSurvivesCancellation(t *testing.T) {
	s, _ := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err := withTx(ctx, s.db, func(*sql.Tx) error {
		calls++
		cancel()
		return ErrCorrupt
	})
	if !errors.Is(err, ErrCorrupt) || errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("callback failure replaced: err=%v calls=%d", err, calls)
	}
}

func TestTransactionDriverCommitFailureSurvivesCancellation(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{"disk-full", transactionCommitCode(13), ErrDiskFull},
		{"unclassified", errors.New("injected commit failure"), ErrUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fault := &transactionCommitFault{commit: func() error {
				cancel()
				return test.err
			}}
			db := sql.OpenDB(transactionFaultConnector{func(context.Context) (driver.Tx, error) {
				return fault, nil
			}})
			defer db.Close()
			calls := 0
			err := withTx(ctx, db, func(*sql.Tx) error {
				calls++
				return nil
			})
			if !errors.Is(err, test.want) || errors.Is(err, context.Canceled) || calls != 1 || fault.commits != 1 {
				t.Fatalf("commit failure replaced or replayed: err=%v calls=%d commits=%d", err, calls, fault.commits)
			}
		})
	}
}

func TestTransactionBeginInterruptRequiresCancelledContext(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		cancel   bool
		deadline bool
		want     error
	}{
		{"cancelled-interrupt", transactionCommitCode(9), true, false, context.Canceled},
		{"deadline-interrupt", transactionCommitCode(9), false, true, context.DeadlineExceeded},
		{"uncancelled-interrupt", transactionCommitCode(9), false, false, ErrUnavailable},
		{"cancelled-disk-full", transactionCommitCode(13), true, false, ErrDiskFull},
		{"cancelled-corrupt", transactionCommitCode(11), true, false, ErrCorrupt},
		{"cancelled-generic", errors.New("injected begin failure"), true, false, ErrUnavailable},
		{"cancelled-completed-transaction", sql.ErrTxDone, true, false, ErrUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			begins, calls := 0, 0
			db := sql.OpenDB(transactionFaultConnector{func(ctx context.Context) (driver.Tx, error) {
				begins++
				if test.cancel {
					cancel()
				}
				if test.deadline {
					<-ctx.Done()
				}
				return nil, test.err
			}})
			defer db.Close()
			if test.deadline {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 100*time.Millisecond)
				defer stop()
			}
			err := withTx(ctx, db, func(*sql.Tx) error {
				calls++
				return nil
			})
			if err != test.want || begins != 1 || calls != 0 {
				t.Fatalf("begin failure replaced or replayed: err=%v want=%v begins=%d calls=%d", err, test.want, begins, calls)
			}
			if test.cancel && ctx.Err() != context.Canceled || test.deadline && ctx.Err() != context.DeadlineExceeded || !test.cancel && !test.deadline && ctx.Err() != nil {
				t.Fatalf("unexpected driver cancellation state: %v", ctx.Err())
			}
		})
	}
}

type transactionCommitCode int

func (transactionCommitCode) Error() string { return "injected coded commit failure" }
func (e transactionCommitCode) Code() int   { return int(e) }

type transactionFaultConnector struct {
	begin func(context.Context) (driver.Tx, error)
}

func (c transactionFaultConnector) Connect(context.Context) (driver.Conn, error) {
	return transactionFaultConnection{c.begin}, nil
}
func (transactionFaultConnector) Driver() driver.Driver { return transactionCommitDriver{} }

type transactionCommitDriver struct{}

func (transactionCommitDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("unexpected direct driver open")
}

type transactionFaultConnection struct {
	begin func(context.Context) (driver.Tx, error)
}

func (transactionFaultConnection) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}
func (transactionFaultConnection) Close() error { return nil }
func (c transactionFaultConnection) Begin() (driver.Tx, error) {
	return c.begin(context.Background())
}
func (c transactionFaultConnection) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return c.begin(ctx)
}

type transactionCommitFault struct {
	commit  func() error
	commits int
}

func (f *transactionCommitFault) Commit() error {
	f.commits++
	return f.commit()
}
func (*transactionCommitFault) Rollback() error { return nil }
