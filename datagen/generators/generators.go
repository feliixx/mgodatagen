// Package generators used to create bson encoded random data.
//
// Relevant documentation:
//
//     http://bsonspec.org/#/specification
//
//
// It was created as part of mgodatagen, but may be used on its own.
package generators

import (
	"github.com/MichaelTJones/pcg"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// DocumentGenerator is a Generator for creating random bson documents
type DocumentGenerator struct {
	// Buffer holds the document bytes
	Buffer *DocBuffer
	// list of all generators used to create the document. The resulting document
	// will have n keys where 0 < n < len(Generators)
	Generators []Generator
}

// Generate creates a new bson document and returns it as a slice of bytes
func (g *DocumentGenerator) Generate() []byte {
	g.Buffer.Truncate(4)
	for _, gen := range g.Generators {
		if gen.Exists() {
			if gen.Type() != bson.TypeNull {
				g.Buffer.WriteSingleByte(byte(gen.Type()))
				g.Buffer.Write(gen.Key())
				g.Buffer.WriteSingleByte(byte(0))
			}
			gen.EncodeValue()
		}
	}
	g.Buffer.WriteSingleByte(byte(0))
	g.Buffer.WriteAt(0, int32Bytes(int32(g.Buffer.Len())))
	return g.Buffer.Bytes()
}

// Add append a new Generator to the DocumentGenerator. The generator EncodeValue() method
// must write to the same DocBuffer as the DocumentGenerator g
func (g *DocumentGenerator) Add(generator Generator) {
	if generator != nil {
		g.Generators = append(g.Generators, generator)
	}
}

// Generator is an interface for generator that can be used to
// generate random value of a specific type, and encode them in bson
// format
type Generator interface {
	// Key returns the element key
	Key() []byte
	// Type returns the bson type of the element as defined in bson spec: http://bsonspec.org/
	Type() bsontype.Type
	// Exists returns true if the generation should be performed.
	Exists() bool
	// EncodeValue encodes a random value in bson and write it to a DocBuffer
	EncodeValue()
	// EncodeToString encodes a random value as a string and write it to a DocBuffer
	EncodeValueAsString()
}

// base implements Key(), Type(), Exists() and EncodeValueAsString() methods. Intended to be
// embedded in each generator
type base struct {
	key []byte
	// probability that the element doesn't exist
	nullPercentage uint32
	bsonType       bsontype.Type
	buffer         *DocBuffer
	pcg32          *pcg.PCG32
}

// newBase returns a new base
func newBase(key string, nullPercentage uint32, bsonType bsontype.Type, out *DocBuffer, pcg32 *pcg.PCG32) base {
	return base{
		key:            []byte(key),
		nullPercentage: nullPercentage,
		bsonType:       bsonType,
		buffer:         out,
		pcg32:          pcg32,
	}
}

func (g *base) Key() []byte { return g.key }

func (g *base) Type() bsontype.Type { return g.bsonType }

// if a generator has a nullPercentage of 10%, this method will return
// true ~90% of the time, and false ~10% of the time
func (g *base) Exists() bool {
	if g.nullPercentage == 0 {
		return true
	}
	// get the last 10 bits of a random int32 to get a number between 0 and 1023,
	// and compare it to nullPercentage * 10
	return g.pcg32.Random()>>22 >= g.nullPercentage
}

func (g *base) EncodeValueAsString() {}
