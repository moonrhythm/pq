package pq

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

type stmt struct {
	cn   *conn
	name string
	rowsHeader
	colFmtData []byte
	paramTyps  []oid.Oid
	closed     bool
}

func (st *stmt) Close() error {
	if st.closed {
		return nil
	}
	if err := st.cn.err.get(); err != nil {
		return err
	}

	w := st.cn.writeBuf(proto.Close)
	w.byte(proto.Sync)
	w.string(st.name)
	err := st.cn.send(w)
	if err != nil {
		return st.cn.handleError(err)
	}
	err = st.cn.send(st.cn.writeBuf(proto.Sync))
	if err != nil {
		return st.cn.handleError(err)
	}

	t, _, err := st.cn.recv1()
	if err != nil {
		return st.cn.handleError(err)
	}
	if t != proto.CloseComplete {
		st.cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("pq: unexpected close response: %q", t)
	}
	st.closed = true

	t, r, err := st.cn.recv1()
	if err != nil {
		return st.cn.handleError(err)
	}
	if t != proto.ReadyForQuery {
		st.cn.err.set(driver.ErrBadConn)
		return fmt.Errorf("pq: expected ready for query, but got: %q", t)
	}
	st.cn.processReadyForQuery(r)

	return nil
}

// Never called; required to satisfy [driver.Stmt]. [database/sql] selects
// QueryContext instead.
func (st *stmt) Query(v []driver.Value) (driver.Rows, error) { panic("stmt.Query") }

// Implement [driver.StmtQueryContext].
func (st *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	finish := st.cn.watchCancel(ctx, true)
	if err := st.cn.err.get(); err != nil {
		return nil, err
	}

	err := st.exec(args)
	if err != nil {
		finish()
		return nil, st.cn.handleError(err)
	}

	return &rows{
		cn:         st.cn,
		rowsHeader: st.rowsHeader,
		finish:     finish,
	}, nil
}

// Never called; required to satisfy [driver.Stmt]. [database/sql] selects
// ExecContext instead.
func (st *stmt) Exec(v []driver.Value) (driver.Result, error) { panic("stmt.Exec") }

// Implement [driver.StmtExecContext].
func (st *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	defer st.cn.watchCancel(ctx, true)()
	if err := st.cn.err.get(); err != nil {
		return nil, err
	}

	err := st.exec(args)
	if err != nil {
		return nil, st.cn.handleError(err)
	}
	res, _, err := st.cn.readExecuteResponse("simple query")
	return res, st.cn.handleError(err)
}

func (st *stmt) exec(v []driver.NamedValue) error {
	if len(v) >= 65536 {
		return fmt.Errorf("pq: got %d parameters but PostgreSQL only supports 65535 parameters", len(v))
	}
	if len(v) != len(st.paramTyps) {
		return fmt.Errorf("pq: got %d parameters but the statement requires %d", len(v), len(st.paramTyps))
	}

	cn := st.cn
	w := cn.writeBuf(proto.Bind)
	w.byte(0) // unnamed portal
	w.string(st.name)

	if cn.cfg.BinaryParameters {
		err := cn.sendBinaryParameters(w, v)
		if err != nil {
			return err
		}
	} else {
		w.int16(0)
		w.int16(len(v))
		for i, x := range v {
			if err := encodeInto(w, x.Value, st.paramTyps[i]); err != nil {
				return err
			}
		}
	}
	w.bytes(st.colFmtData)

	w.next(proto.Execute)
	w.byte(0)
	w.int32(0)

	w.next(proto.Sync)
	err := cn.send(w)
	if err != nil {
		return err
	}
	err = cn.readBindResponse()
	if err != nil {
		return err
	}
	return cn.postExecuteWorkaround()
}

func (st *stmt) NumInput() int {
	return len(st.paramTyps)
}
