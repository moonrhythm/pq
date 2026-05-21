package pq

import "testing"

func BenchmarkWriteBufInt32(b *testing.B) {
	wb := &writeBuf{buf: make([]byte, 0, 4*16)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb.buf = wb.buf[:0]
		for j := 0; j < 16; j++ {
			wb.int32(j)
		}
	}
}

func BenchmarkWriteBufInt16(b *testing.B) {
	wb := &writeBuf{buf: make([]byte, 0, 2*16)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wb.buf = wb.buf[:0]
		for j := 0; j < 16; j++ {
			wb.int16(j)
		}
	}
}
