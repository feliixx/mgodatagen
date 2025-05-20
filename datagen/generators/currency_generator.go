package generators

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/MichaelTJones/pcg"
)

// Generator for creating random currency value (decimal128), but limited
type currencyGenerator struct {
	base
	min   int64
	max   int64
	scale int
	pcg64 *pcg.PCG64
}

func newCurrencyGenerator(config *Config, base base, pcg64 *pcg.PCG64) (g Generator, err error) {
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

	if min < 0 {
		return nil, errors.New("currently supports 'min' >= 0")
	}
	if min > max {
		return nil, errors.New("make sure that 'max' >= 'min'")
	}
	if min == max {
		return newConstantGenerator(base, max)
	}
	return &currencyGenerator{
			base:  base,
			min:   min,
			max:   max + 1,
			scale: 2,
			pcg64: pcg64},
		nil
}

func (g *currencyGenerator) EncodeValue() {
	v := uint64(g.boundedInt64())
	m := uint64(1) << 63 //mask for sign bit
	neg := v & m
	high := uint64Bytes(neg | uint64((6176-g.scale)<<49))
	low := uint64Bytes(v & (^neg))
	g.buffer.Write(high)
	g.buffer.Write(low)
}

func (g *currencyGenerator) EncodeValueAsString() {
	g.buffer.WriteString(strconv.FormatInt(g.boundedInt64(), 10))
}

func (g *currencyGenerator) boundedInt64() int64 {
	return int64(g.pcg64.Bounded(uint64(g.max-g.min))) + g.min
}
