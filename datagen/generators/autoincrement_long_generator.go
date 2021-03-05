package generators

import (
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
)

// Generator for creating auto-incremented int64
type autoIncrementLongGenerator struct {
	base
	counter int64
}

func newAutoIncrementLongGenerator(config *Config, base base) (Generator, error) {
	base.bsonType = bson.TypeInt64
	return &autoIncrementLongGenerator{
		base:    base,
		counter: config.StartLong,
	}, nil
}

func (g *autoIncrementLongGenerator) EncodeValue() {
	g.buffer.Write(int64Bytes(g.counter))
	g.counter++
}

func (g *autoIncrementLongGenerator) EncodeValueAsString() {
	val := strconv.FormatInt(g.counter, 10)
	g.buffer.Write([]byte(val))
	g.counter++
}
