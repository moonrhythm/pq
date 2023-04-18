package pq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"time"
)

const (
	watchCancelDialContextTimeout = time.Second * 10
)

// Implement the "QueryerContext" interface
func (cn *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	finish := cn.watchCancel(ctx)
	r, err := cn.query(query, args)
	if err != nil {
		if finish != nil {
			finish()
		}
		return nil, err
	}
	r.finish = finish
	return r, nil
}

// Implement the "ExecerContext" interface
func (cn *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (res driver.Result, err error) {
	if finish := cn.watchCancel(ctx); finish != nil {
		defer finish()
	}

	if err := cn.err.get(); err != nil {
		return nil, err
	}
	defer cn.errRecover(&err)

	// Check to see if we can use the "simpleExec" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		// ignore commandTag, our caller doesn't care
		r, _, err := cn.simpleExec(query)
		return r, err
	}

	if cn.binaryParameters {
		cn.sendBinaryModeQuery(query, args)

		cn.readParseResponse()
		cn.readBindResponse()
		cn.readPortalDescribeResponse()
		cn.postExecuteWorkaround()
		res, _, err = cn.readExecuteResponse("Execute")
		return res, err
	}
	// Use the unnamed statement to defer planning until bind
	// time, or else value-based selectivity estimates cannot be
	// used.
	st := cn.prepareTo(query, "")
	r, err := st.ExecContext(ctx, args)
	if err != nil {
		panic(err)
	}
	return r, err
}

// Implement the "ConnPrepareContext" interface
func (cn *conn) PrepareContext(ctx context.Context, query string) (_ driver.Stmt, err error) {
	if finish := cn.watchCancel(ctx); finish != nil {
		defer finish()
	}

	if err := cn.err.get(); err != nil {
		return nil, err
	}
	defer cn.errRecover(&err)

	if len(query) >= 4 && strings.EqualFold(query[:4], "COPY") {
		s, err := cn.prepareCopyIn(query)
		if err == nil {
			cn.inCopy = true
		}
		return s, err
	}
	return cn.prepareTo(query, cn.gname()), nil
}

// Implement the "ConnBeginTx" interface
func (cn *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var mode string

	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
		// Don't touch mode: use the server's default
	case sql.LevelReadUncommitted:
		mode = " ISOLATION LEVEL READ UNCOMMITTED"
	case sql.LevelReadCommitted:
		mode = " ISOLATION LEVEL READ COMMITTED"
	case sql.LevelRepeatableRead:
		mode = " ISOLATION LEVEL REPEATABLE READ"
	case sql.LevelSerializable:
		mode = " ISOLATION LEVEL SERIALIZABLE"
	default:
		return nil, fmt.Errorf("pq: isolation level not supported: %d", opts.Isolation)
	}

	if opts.ReadOnly {
		mode += " READ ONLY"
	} else {
		mode += " READ WRITE"
	}

	tx, err := cn.begin(mode)
	if err != nil {
		return nil, err
	}
	cn.txnFinish = cn.watchCancel(ctx)
	return tx, nil
}

func (cn *conn) Ping(ctx context.Context) error {
	if finish := cn.watchCancel(ctx); finish != nil {
		defer finish()
	}
	rows, err := cn.simpleQuery(";")
	if err != nil {
		return driver.ErrBadConn // https://golang.org/pkg/database/sql/driver/#Pinger
	}
	rows.Close()
	return nil
}

func (cn *conn) watchCancel(ctx context.Context) func() {
	if done := ctx.Done(); done != nil {
		finished := make(chan struct{}, 1)
		go func() {
			select {
			case <-done:
				select {
				case finished <- struct{}{}:
				default:
					// We raced with the finish func, let the next query handle this with the
					// context.
					return
				}

				// Set the connection state to bad so it does not get reused.
				cn.err.set(ctx.Err())

				// At this point the function level context is canceled,
				// so it must not be used for the additional network
				// request to cancel the query.
				// Create a new context to pass into the dial.
				ctxCancel, cancel := context.WithTimeout(context.Background(), watchCancelDialContextTimeout)
				defer cancel()

				_ = cn.cancel(ctxCancel)
			case <-finished:
			}
		}()
		return func() {
			select {
			case <-finished:
				cn.err.set(ctx.Err())
				cn.Close()
			case finished <- struct{}{}:
			}
		}
	}
	return nil
}

func (cn *conn) cancel(ctx context.Context) error {
	// Create a new values map (copy). This makes sure the connection created
	// in this method cannot write to the same underlying data, which could
	// cause a concurrent map write panic. This is necessary because cancel
	// is called from a goroutine in watchCancel.
	o := make(values)
	for k, v := range cn.opts {
		o[k] = v
	}

	c, err := dial(ctx, cn.dialer, o)
	if err != nil {
		return err
	}
	defer c.Close()

	{
		can := conn{
			c: c,
		}
		err = can.ssl(o)
		if err != nil {
			return err
		}

		w := can.writeBuf(0)
		w.int32(80877102) // cancel request code
		w.int32(cn.processID)
		w.int32(cn.secretKey)

		if err := can.sendStartupPacket(w); err != nil {
			return err
		}
	}

	// Read until EOF to ensure that the server received the cancel.
	{
		_, err := io.Copy(io.Discard, c)
		return err
	}
}

// Implement the "StmtQueryContext" interface
func (st *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	finish := st.watchCancel(ctx)
	r, err := st.query(args)
	if err != nil {
		if finish != nil {
			finish()
		}
		return nil, err
	}
	r.finish = finish
	return r, nil
}

// Implement the "StmtExecContext" interface
func (st *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (res driver.Result, err error) {
	if finish := st.watchCancel(ctx); finish != nil {
		defer finish()
	}

	if err := st.cn.err.get(); err != nil {
		return nil, err
	}
	defer st.cn.errRecover(&err)

	st.exec(args)
	res, _, err = st.cn.readExecuteResponse("simple query")
	return res, err
}

// watchCancel is implemented on stmt in order to not mark the parent conn as bad
func (st *stmt) watchCancel(ctx context.Context) func() {
	if done := ctx.Done(); done != nil {
		finished := make(chan struct{})
		go func() {
			select {
			case <-done:
				// At this point the function level context is canceled,
				// so it must not be used for the additional network
				// request to cancel the query.
				// Create a new context to pass into the dial.
				ctxCancel, cancel := context.WithTimeout(context.Background(), watchCancelDialContextTimeout)
				defer cancel()

				_ = st.cancel(ctxCancel)
				finished <- struct{}{}
			case <-finished:
			}
		}()
		return func() {
			select {
			case <-finished:
			case finished <- struct{}{}:
			}
		}
	}
	return nil
}

func (st *stmt) cancel(ctx context.Context) error {
	return st.cn.cancel(ctx)
}
