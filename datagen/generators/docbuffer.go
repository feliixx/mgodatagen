package generators

import (
	"math"
)

// DocBuffer is a wrapper around a slice of bytes. It provides
// method similar to bytes.Buffer, plus a specific method WriteAt() to
// write at a specific position of the underlying slice of bytes
type DocBuffer struct {
	buf []byte
}

// NewDocBuffer returns a new DocBuffer
func NewDocBuffer() *DocBuffer {
	return &DocBuffer{
		buf: make([]byte, 0, 256),
	}
}

// Truncate discards all but the first n bytes from the buffer
func (e *DocBuffer) Truncate(n int) {
	for len(e.buf) < n {
		e.buf = append(e.buf, byte(0))
	}
	e.buf = e.buf[:n]
}

// Write appends bytes to the buffer
func (e *DocBuffer) Write(b []byte) {
	e.buf = append(e.buf, b...)
}

func (e *DocBuffer) WriteString(s string) {
	e.buf = append(e.buf, s...)
}

// WriteSingleByte appends a single byte to the buffer
func (e *DocBuffer) WriteSingleByte(b byte) {
	e.buf = append(e.buf, b)
}

// WriteAt writes bytes to the buffer at a specific
// position. This method will panic if off is greater than
// the length of the buffer, or if b is larger
// than the buffer
func (e *DocBuffer) WriteAt(off int, b []byte) {
	copy(e.buf[off:off+len(b)], b)
}

// Reserve appends 4 bytes to the buffer
func (e *DocBuffer) Reserve() {
	e.buf = append(e.buf, byte(0), byte(0), byte(0), byte(0))
}

// Bytes returns the content of the buffer. The resulting slice of
// should be copied before being used, otherwise it's content may
// be change
func (e *DocBuffer) Bytes() []byte {
	return e.buf
}

// Len returns the length of the buffer
func (e *DocBuffer) Len() int {
	return len(e.buf)
}

func int32Bytes(v int32) []byte {
	u := uint32(v)
	return uint32Bytes(u)
}

func uint32Bytes(v uint32) []byte {
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
}

func int64Bytes(v int64) []byte {
	u := uint64(v)
	return uint64Bytes(u)
}

func uint64Bytes(v uint64) []byte {
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24),
		byte(v >> 32), byte(v >> 40), byte(v >> 48), byte(v >> 56)}
}

func float64Bytes(v float64) []byte {
	return int64Bytes(int64(math.Float64bits(v)))
}
