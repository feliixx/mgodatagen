package generators

// Generator for creating random bool
type boolGenerator struct {
	base
}

func newBoolGenerator(base base) (Generator, error) {
	return &boolGenerator{base: base}, nil
}

func (g *boolGenerator) EncodeValue() {
	g.buffer.WriteSingleByte(byte(g.randomByte()))
}

func (g *boolGenerator) EncodeValueAsString() {
	if g.randomByte() == 0 {
		g.buffer.Write([]byte("true"))
	}
	g.buffer.Write([]byte("false"))
}

func (g *boolGenerator) randomByte() byte {
	return byte(g.pcg32.Random() & 0x01)
}
