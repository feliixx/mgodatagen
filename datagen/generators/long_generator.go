package generators

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
)

// Generator for creating random int64 between `Min` and `Max`
type longGenerator struct {
	base
	min  int64
	max  int64
	rand *rand.Rand
}

func newLongGenerator(config *Config, base base, rand *rand.Rand) (g Generator, err error) {

	min, max := int64(0), int64(math.MaxInt64-2)

	if config.Min != "" {
		min, err = strconv.ParseInt(string(config.Min), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("can't parse number '%s' as a long:\n%w", config.Min, err)
		}
	}

	if config.Max != "" {
		max, err = strconv.ParseInt(string(config.Max), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("can't parse number '%s' as a long:\n%w", config.Max, err)
		}
	}

	if min > max {
		return nil, errors.New("make sure that 'max' >= 'min'")
	}
	if min == max {
		return newConstantGenerator(base, max)
	}
	return &longGenerator{
		base: base,
		min:  min,
		max:  max + 1,
		rand: rand,
	}, nil
}

func (g *longGenerator) EncodeValue() {
	g.buffer.Write(int64Bytes(g.boundedInt64()))
}

func (g *longGenerator) EncodeValueAsString() {
	g.buffer.WriteString(strconv.FormatInt(g.boundedInt64(), 10))
}

func (g *longGenerator) boundedInt64() int64 {
	return g.rand.Int64N(g.max-g.min) + g.min
}
