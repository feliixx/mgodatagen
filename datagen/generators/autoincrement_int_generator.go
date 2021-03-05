package generators

import (
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
)

// Generator for creating auto-incremented int32
type autoIncrementIntGenerator struct {
	base
	counter int32
}

func newAutoIncrementIntGenerator(config *Config, base base) (Generator, error) {
	base.bsonType = bson.TypeInt32
	return &autoIncrementIntGenerator{
		base:    base,
		counter: config.StartInt,
	}, nil
}

func (g *autoIncrementIntGenerator) EncodeValue() {
	g.buffer.Write(int32Bytes(g.counter))
	g.counter++
}

func (g *autoIncrementIntGenerator) EncodeValueAsString() {
	val := strconv.Itoa(int(g.counter))
	g.buffer.Write([]byte(val))
	g.counter++
}
