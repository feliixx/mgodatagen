package generators

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
)

// Generator for creating random float64 between `Min` and `Max`
type doubleGenerator struct {
	base
	mean   float64
	stdDev float64
	rand   *rand.Rand
}

func newDoubleGenerator(config *Config, base base, rand *rand.Rand) (g Generator, err error) {

	min, max := 0.0, math.MaxFloat64-2

	if config.Min != "" {
		min, err = strconv.ParseFloat(string(config.Min), 64)
		if err != nil {
			return nil, fmt.Errorf("can't parse number '%s' as a double:\n%w", config.Min, err)
		}
	}

	if config.Max != "" {
		max, err = strconv.ParseFloat(string(config.Max), 64)
		if err != nil {
			return nil, fmt.Errorf("can't parse number '%s' as a double:\n%w", config.Max, err)
		}
	}

	if min > max {
		return nil, errors.New("make sure that 'max' >= 'min'")
	}
	if min == max {
		return newConstantGenerator(base, max)
	}
	return &doubleGenerator{
		base:   base,
		mean:   min,
		stdDev: (max - min) / 2,
		rand:   rand,
	}, nil
}

func (g *doubleGenerator) EncodeValue() {
	g.buffer.Write(float64Bytes(g.boundedFloat64()))
}

func (g *doubleGenerator) EncodeValueAsString() {
	g.buffer.WriteString(strconv.FormatFloat(g.boundedFloat64(), 'f', 10, 64))
}

func (g *doubleGenerator) boundedFloat64() float64 {
	return float64(g.rand.Uint64())/(1<<64)*g.stdDev + g.mean
}
