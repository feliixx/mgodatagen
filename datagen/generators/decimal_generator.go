package generators

import "github.com/MichaelTJones/pcg"

// Generator for creating random decimal128
type decimal128Generator struct {
	base
	pcg64 *pcg.PCG64
}

func newDecimalGenerator(base base, pcg64 *pcg.PCG64) (Generator, error) {
	return &decimal128Generator{
			base:  base,
			pcg64: pcg64},
		nil
}

func (g *decimal128Generator) EncodeValue() {
	b := uint64Bytes(g.pcg64.Random())
	g.buffer.Write(b)
	g.buffer.Write(b)
}
