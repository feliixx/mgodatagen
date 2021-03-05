package generators

import "errors"

// Generator for creating random binary data
type binaryDataGenerator struct {
	base
	minLength uint32
	maxLength uint32
}

func newBinaryGenerator(config *Config, base base) (Generator, error) {
	if config.MinLength < 0 || config.MinLength > config.MaxLength {
		return nil, errors.New("make sure that 'minLength' >= 0 and 'minLength' < 'maxLength'")
	}
	return &binaryDataGenerator{
		base:      base,
		maxLength: uint32(config.MaxLength),
		minLength: uint32(config.MinLength),
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
