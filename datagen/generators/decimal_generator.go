package generators

import (
	"math/rand/v2"
	"strconv"
)

// Generator for creating random decimal128
type decimal128Generator struct {
	base
	rand *rand.Rand
}

func newDecimalGenerator(base base, rand *rand.Rand) (Generator, error) {
	return &decimal128Generator{
		base: base,
		rand: rand,
	}, nil
}

func (g *decimal128Generator) EncodeValue() {
	b := uint64Bytes(g.rand.Uint64())
	g.buffer.Write(b)
	g.buffer.Write(b)
}

func (g *decimal128Generator) EncodeValueAsString() {
	s := strconv.Itoa(g.rand.Int())
	g.buffer.WriteString(s)
	g.buffer.WriteString(s)
}
