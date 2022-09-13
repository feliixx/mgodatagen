package generators

import (
	"fmt"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
)

// Generator for creating auto-incremented int32
type autoIncrementIntGenerator struct {
	base
	counter int32
}

func newAutoIncrementIntGenerator(config *Config, base base) (g Generator, err error) {
	base.bsonType = bson.TypeInt32

	start := int64(0)
	if config.Start != "" {
		start, err = strconv.ParseInt(string(config.Start), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("can't parse number '%s' as an int:\n%w", config.Start, err)
		}
	}

	return &autoIncrementIntGenerator{
		base:    base,
		counter: int32(start),
	}, nil
}

func (g *autoIncrementIntGenerator) EncodeValue() {
	g.buffer.Write(int32Bytes(g.counter))
	g.counter++
}

func (g *autoIncrementIntGenerator) EncodeValueAsString() {
	val := strconv.Itoa(int(g.counter))
	g.buffer.WriteString(val)
	g.counter++
}
