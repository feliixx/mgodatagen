package generators

import (
	"errors"
	"strconv"
)

// Generator for creating random int32 between `Min` and `Max`
type intGenerator struct {
	base
	min int32
	max int32
}

func newIntGenerator(config *Config, base base) (Generator, error) {
	if config.MaxInt < config.MinInt {
		return nil, errors.New("make sure that 'maxInt' >= 'minInt'")
	}
	if config.MinInt == config.MaxInt {
		return newConstantGenerator(base, config.MaxInt)
	}
	return &intGenerator{
		base: base,
		min:  config.MinInt,
		max:  config.MaxInt + 1,
	}, nil
}

func (g *intGenerator) EncodeValue() {
	g.buffer.Write(int32Bytes(g.boundedInt32()))
}

func (g *intGenerator) EncodeValueAsString() {
	g.buffer.Write([]byte(strconv.Itoa(int(g.boundedInt32()))))
}

func (g *intGenerator) boundedInt32() int32 {
	return int32(g.pcg32.Bounded(uint32(g.max-g.min))) + g.min
}
