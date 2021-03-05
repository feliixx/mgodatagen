package generators

import (
	"errors"
	"strconv"

	"github.com/MichaelTJones/pcg"
)

// Generator for creating random float64 between `Min` and `Max`
type doubleGenerator struct {
	base
	mean   float64
	stdDev float64
	pcg64  *pcg.PCG64
}

func newDoubleGenerator(config *Config, base base, pcg64 *pcg.PCG64) (Generator, error) {
	if config.MaxDouble < config.MinDouble {
		return nil, errors.New("make sure that 'maxDouble' >= 'minDouble'")
	}
	if config.MinDouble == config.MaxDouble {
		return newConstantGenerator(base, config.MaxDouble)
	}
	return &doubleGenerator{
		base:   base,
		mean:   config.MinDouble,
		stdDev: (config.MaxDouble - config.MinDouble) / 2,
		pcg64:  pcg64,
	}, nil
}

func (g *doubleGenerator) EncodeValue() {
	g.buffer.Write(float64Bytes(g.boundedFloat64()))
}

func (g *doubleGenerator) EncodeValueAsString() {
	g.buffer.Write([]byte(strconv.FormatFloat(g.boundedFloat64(), 'f', 10, 64)))
}

func (g *doubleGenerator) boundedFloat64() float64 {
	return float64(g.pcg64.Random())/(1<<64)*g.stdDev + g.mean
}
