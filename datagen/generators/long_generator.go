package generators

import (
	"errors"
	"strconv"

	"github.com/MichaelTJones/pcg"
)

// Generator for creating random int64 between `Min` and `Max`
type longGenerator struct {
	base
	min   int64
	max   int64
	pcg64 *pcg.PCG64
}

func newLongGenerator(config *Config, base base, pcg64 *pcg.PCG64) (Generator, error) {
	if config.MaxLong < config.MinLong {
		return nil, errors.New("make sure that 'maxLong' >= 'minLong'")
	}
	if config.MinLong == config.MaxLong {
		return newConstantGenerator(base, config.MaxLong)
	}
	return &longGenerator{
		base:  base,
		min:   config.MinLong,
		max:   config.MaxLong + 1,
		pcg64: pcg64,
	}, nil
}

func (g *longGenerator) EncodeValue() {
	g.buffer.Write(int64Bytes(g.boundedInt64()))
}

func (g *longGenerator) EncodeValueAsString() {
	g.buffer.Write([]byte(strconv.FormatInt(g.boundedInt64(), 10)))
}

func (g *longGenerator) boundedInt64() int64 {
	return int64(g.pcg64.Bounded(uint64(g.max-g.min))) + g.min
}
