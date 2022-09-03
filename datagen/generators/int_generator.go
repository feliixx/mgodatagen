package generators

import (
	"errors"
	"fmt"
	"math"
	"strconv"
)

// Generator for creating random int32 between `Min` and `Max`
type intGenerator struct {
	base
	min int32
	max int32
}

func newIntGenerator(config *Config, base base) (g Generator, err error) {

	min, max := int64(0), int64(math.MaxInt32-2)

	if config.Min != "" {
		min, err = strconv.ParseInt(string(config.Min), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("can't parse number '%s' as an int:\n%w", config.Min, err)
		}
	}

	if config.Max != "" {
		max, err = strconv.ParseInt(string(config.Max), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("can't parse number '%s' as an int:\n%w", config.Max, err)
		}
	}

	if min > max {
		return nil, errors.New("make sure that 'max' >= 'min'")
	}
	if min == max {
		return newConstantGenerator(base, max)
	}

	return &intGenerator{
		base: base,
		min:  int32(min),
		max:  int32(max) + 1,
	}, nil
}

func (g *intGenerator) EncodeValue() {
	g.buffer.Write(int32Bytes(g.boundedInt32()))
}

func (g *intGenerator) EncodeValueAsString() {
	g.buffer.WriteString(strconv.Itoa(int(g.boundedInt32())))
}

func (g *intGenerator) boundedInt32() int32 {
	return int32(g.pcg32.Bounded(uint32(g.max-g.min))) + g.min
}
