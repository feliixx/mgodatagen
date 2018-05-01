package generators

import "math"

// Encoder is a wrapper around a slice of bytes. It provides
// method similar to bytes.Buffer, plus a specific method WriteAt() to
// write buf at a specific position of the underlying slice of bytes
type Encoder struct {
	buf []byte
}

// NewEncoder returns a new encoder seeded with time.Now()
func NewEncoder() *Encoder {
	return &Encoder{
		buf: make([]byte, 0, 256),
	}
}

// Truncate discards all but the first n bytes from the buffer
func (e *Encoder) Truncate(n int) {
	for len(e.buf) < n {
		e.buf = append(e.buf, byte(0))
	}
	e.buf = e.buf[:n]
}

// Write appends bytes to the buffer
func (e *Encoder) Write(b []byte) {
	e.buf = append(e.buf, b...)
}

// WriteSingleByte appends a single byte to the buffer
func (e *Encoder) WriteSingleByte(b byte) {
	e.buf = append(e.buf, b)
}

// WriteAt writes bytes to the buffer at a specific
// position
func (e *Encoder) WriteAt(startPos int, b []byte) {
	copy(e.buf[startPos:startPos+len(b)], b)
}

// Reserve add 4 bytes to the buffer
func (e *Encoder) Reserve() {
	e.buf = append(e.buf, byte(0), byte(0), byte(0), byte(0))
}

// Bytes returns the content of the buffer
func (e *Encoder) Bytes() []byte {
	return e.buf
}

// Len returns the length of the buffer
func (e *Encoder) Len() int {
	return len(e.buf)
}

// int32Bytes convert an int32 into an array of bytes
func int32Bytes(v int32) []byte {
	u := uint32(v)
	return uint32Bytes(u)
}

// uint32Bytes returns an uint32 into an array of bytes
func uint32Bytes(v uint32) []byte {
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
}

// int64Bytes convert an int64 into an array of bytes
func int64Bytes(v int64) []byte {
	u := uint64(v)
	return uint64Bytes(u)
}

// uint64Bytes returns an uint64 into an array of bytes
func uint64Bytes(v uint64) []byte {
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24),
		byte(v >> 32), byte(v >> 40), byte(v >> 48), byte(v >> 56)}
}

// float64Bytes convert an int32 into an array of bytes
func float64Bytes(v float64) []byte {
	return int64Bytes(int64(math.Float64bits(v)))
}
