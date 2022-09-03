package generators

import (
	"errors"
	"fmt"
	"strconv"
)

// Generator for creating random binary data
type binaryDataGenerator struct {
	base
	minLength uint32
	maxLength uint32
}

func newBinaryGenerator(config *Config, base base) (g Generator, err error) {

	min, max := uint64(0), uint64(10)

	if config.MinLength != "" {
		min, err = strconv.ParseUint(string(config.MinLength), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("can't parse number '%s' as an uint32:\n%w", config.MinLength, err)
		}
	}

	if config.MaxLength != "" {
		max, err = strconv.ParseUint(string(config.MaxLength), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("can't parse number '%s' as an uint32:\n%w", config.MaxLength, err)
		}
	}

	if min > max {
		return nil, errors.New("make sure that 'minLength' < 'maxLength'")
	}

	return &binaryDataGenerator{
		base:      base,
		minLength: uint32(min),
		maxLength: uint32(max),
	}, nil
}

// legacy type binary instead of 0x05
const genericBinaryType = 0x00

func (g *binaryDataGenerator) EncodeValue() {
	length := g.minLength
	if g.minLength != g.maxLength {
		length = g.pcg32.Bounded(g.maxLength-g.minLength+1) + g.minLength
	}
	g.buffer.Write(uint32Bytes(length))
	g.buffer.WriteSingleByte(genericBinaryType)
	end := 4
	for count := 0; count < int(length); count += 4 {
		b := uint32Bytes(g.pcg32.Random())
		if int(length)-count < 4 {
			end = int(length) - count
		}
		g.buffer.Write(b[0:end])
	}
}

func (g *binaryDataGenerator) EncodeValueAsString() {}
