package generators

import (
	"errors"
	"strconv"

	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// Generator for creating random array
type arrayGenerator struct {
	base
	size      int
	generator Generator
}

func newArrayGenerator(config *Config, base base, ci *CollInfo, buffer *DocBuffer) (Generator, error) {
	if config.Size <= 0 {
		return nil, errors.New("make sure that 'size' >= 0")
	}
	g, err := ci.newGenerator(buffer, "", config.ArrayContent)
	if err != nil {
		return nil, err
	}

	// if the generator is of type FromArrayGenerator,
	// use the type of the first Element as global type
	// for the generator
	// => fromArrayGenerator currently has to contain object of
	// the same type, otherwise bson object will be incorrect
	switch g := g.(type) {
	case *fromArrayGenerator:
		// if array is generated with preGenerate(), this step is not needed
		if !g.doNotTruncate {
			g.bsonType = bsontype.Type(g.bsonArray[0][0])
			// do not write first 3 bytes, ie
			// bson type, byte("k"), byte(0) to avoid conflict with
			// array index, because index is the key
			for i := range g.bsonArray {
				g.bsonArray[i] = g.bsonArray[i][3:]
			}
		}
	case *constGenerator:
		g.bsonType = bsontype.Type(g.bsonVal[0])
		// 2: 1 for bson type, and 1 for terminating byte 0x00 after element key
		g.bsonVal = g.bsonVal[2+len(g.Key()):]
	default:
	}

	return &arrayGenerator{
		base:      base,
		size:      config.Size,
		generator: g,
	}, nil
}

// precomputed index. Most of the array
// will be of short length, so precompute
// the first indexes values to avoid calls
// to strconv.Itoa
// Use array as access is faster than with map
var indexesBytes = [10]byte{
	byte(48),
	byte(49),
	byte(50),
	byte(51),
	byte(52),
	byte(53),
	byte(54),
	byte(55),
	byte(56),
	byte(57),
}

func (g *arrayGenerator) EncodeValue() {
	current := g.buffer.Len()
	g.buffer.Reserve()
	// array looks like this:
	// size (byte(index) byte(0) value)... byte(0)
	// where index is a string: ["1", "2", "3"...]
	for i := 0; i < g.size; i++ {
		g.buffer.WriteSingleByte(byte(g.generator.Type()))
		if i < 10 {
			g.buffer.WriteSingleByte(indexesBytes[i])
		} else {
			g.buffer.Write([]byte(strconv.Itoa(i)))
		}
		g.buffer.WriteSingleByte(byte(0))
		g.generator.EncodeValue()
	}
	g.buffer.WriteSingleByte(byte(0))
	g.buffer.WriteAt(current, int32Bytes(int32(g.buffer.Len()-current)))
}
