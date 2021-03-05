package generators

import (
	"errors"
	"fmt"
)

// Generator for creating a random value from an array of user-defined values
type fromArrayGenerator struct {
	base
	size          int
	bsonArray     [][]byte
	strArray      [][]byte
	index         int
	randomOrder   bool
	doNotTruncate bool
}

func newFromArrayGenerator(config *Config, base base) (Generator, error) {

	size := len(config.In)

	if size == 0 {
		return nil, errors.New("'in' array can't be null or empty")
	}

	array := make([][]byte, size)
	arrayStr := make([][]byte, size)
	for i, v := range config.In {
		raw, err := bsonValue(string(base.key), v)
		if err != nil {
			return nil, err
		}
		array[i] = raw
		arrayStr[i] = []byte(fmt.Sprint(v))
	}
	return &fromArrayGenerator{
		base:        base,
		bsonArray:   array,
		strArray:    arrayStr,
		size:        size,
		index:       0,
		randomOrder: config.RandomOrder,
	}, nil
}

func newFromArrayGeneratorWithPregeneratedValues(base base, values [][]byte, doNotTruncate bool) (Generator, error) {
	return &fromArrayGenerator{
		base:          base,
		bsonArray:     values,
		size:          len(values),
		index:         0,
		doNotTruncate: doNotTruncate,
	}, nil
}

func (g *fromArrayGenerator) EncodeValue() {
	g.buffer.Write(g.bsonArray[g.randomIndex()])
}

func (g *fromArrayGenerator) EncodeValueAsString() {
	g.buffer.Write(g.strArray[g.randomIndex()])
}

func (g *fromArrayGenerator) randomIndex() int {

	if g.randomOrder {
		return int(g.base.pcg32.Bounded(uint32(g.size)))
	}

	if g.index == g.size {
		g.index = 0
	}
	i := g.index
	g.index++
	return i
}
