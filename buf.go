package pq

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	"github.com/lib/pq/internal/proto"
	"github.com/lib/pq/oid"
)

// unsafeString returns a string that aliases the bytes of b without copying.
// The caller must guarantee that b is not modified for the lifetime of the
// returned string and that the consumer does not retain the string beyond the
// lifetime of b. Intended for read-only pure functions (e.g. strconv.ParseInt)
// where the cost of a copy would otherwise dominate.
func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

type readBuf []byte

func (b *readBuf) int32() (n int) {
	n = int(int32(binary.BigEndian.Uint32(*b)))
	*b = (*b)[4:]
	return
}

func (b *readBuf) oid() (n oid.Oid) {
	n = oid.Oid(binary.BigEndian.Uint32(*b))
	*b = (*b)[4:]
	return
}

// N.B: this is actually an unsigned 16-bit integer, unlike int32
func (b *readBuf) int16() (n int) {
	n = int(binary.BigEndian.Uint16(*b))
	*b = (*b)[2:]
	return
}

func (b *readBuf) string() string {
	i := bytes.IndexByte(*b, 0)
	if i < 0 {
		panic(errors.New("pq: invalid message format; expected string terminator"))
	}
	s := (*b)[:i]
	*b = (*b)[i+1:]
	return string(s)
}

func (b *readBuf) next(n int) (v []byte) {
	v = (*b)[:n]
	*b = (*b)[n:]
	return
}

func (b *readBuf) byte() byte {
	return b.next(1)[0]
}

type writeBuf struct {
	buf []byte
	pos int
}

func (b *writeBuf) int32(n int) {
	b.buf = binary.BigEndian.AppendUint32(b.buf, uint32(n))
}

func (b *writeBuf) int16(n int) {
	b.buf = binary.BigEndian.AppendUint16(b.buf, uint16(n))
}

func (b *writeBuf) string(s string) {
	b.buf = append(append(b.buf, s...), '\000')
}

func (b *writeBuf) byte(c proto.RequestCode) {
	b.buf = append(b.buf, byte(c))
}

func (b *writeBuf) bytes(v []byte) {
	b.buf = append(b.buf, v...)
}

// reserveLen reserves 4 bytes for a length prefix and returns the position
// of the reserved slot. After writing the payload, call patchLen with the
// returned position to back-patch the payload length.
func (b *writeBuf) reserveLen() int {
	pos := len(b.buf)
	b.buf = append(b.buf, 0, 0, 0, 0)
	return pos
}

// patchLen writes the length of the payload that was appended since
// reserveLen returned pos.
func (b *writeBuf) patchLen(pos int) {
	binary.BigEndian.PutUint32(b.buf[pos:pos+4], uint32(len(b.buf)-pos-4))
}

func (b *writeBuf) wrap() []byte {
	p := b.buf[b.pos:]
	if len(p) > proto.MaxUint32 {
		panic(fmt.Errorf("pq: message too large (%d > math.MaxUint32)", len(p)))
	}
	binary.BigEndian.PutUint32(p, uint32(len(p)))
	return b.buf
}

func (b *writeBuf) next(c proto.RequestCode) {
	p := b.buf[b.pos:]
	if len(p) > proto.MaxUint32 {
		panic(fmt.Errorf("pq: message too large (%d > math.MaxUint32)", len(p)))
	}
	binary.BigEndian.PutUint32(p, uint32(len(p)))
	b.pos = len(b.buf) + 1
	b.buf = append(b.buf, byte(c), 0, 0, 0, 0)
}
