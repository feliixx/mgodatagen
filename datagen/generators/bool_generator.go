package generators

// Generator for creating random bool
type boolGenerator struct {
	base
}

func newBoolGenerator(base base) (Generator, error) {
	return &boolGenerator{base: base}, nil
}

func (g *boolGenerator) Value() {
	g.buffer.WriteSingleByte(byte(g.randomByte()))
}

func (g *boolGenerator) String() string {
	if g.randomByte() == 0 {
		return "true"
	}
	return "false"
}

func (g *boolGenerator) randomByte() byte {
	return byte(g.pcg32.Random() & 0x01)
}
