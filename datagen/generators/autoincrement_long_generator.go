package generators

import (
	"fmt"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
)

// Generator for creating auto-incremented int64
type autoIncrementLongGenerator struct {
	base
	counter int64
}

func newAutoIncrementLongGenerator(config *Config, base base) (g Generator, err error) {
	base.bsonType = bson.TypeInt64

	start := int64(0)
	if config.Start != "" {
		start, err = strconv.ParseInt(string(config.Start), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("can't parse number '%s' as a long:\n%w", config.Start, err)
		}
	}
	return &autoIncrementLongGenerator{
		base:    base,
		counter: start,
	}, nil
}

func (g *autoIncrementLongGenerator) EncodeValue() {
	g.buffer.Write(int64Bytes(g.counter))
	g.counter++
}

func (g *autoIncrementLongGenerator) EncodeValueAsString() {
	val := strconv.FormatInt(g.counter, 10)
	g.buffer.WriteString(val)
	g.counter++
}
