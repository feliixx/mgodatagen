package generators

import (
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
)

type stringUUIDGenerator struct {
	base
}

func newStringUUIDGenerator(base base) (Generator, error) {
	base.bsonType = bson.TypeString
	return &stringUUIDGenerator{base: base}, nil
}

func (g *stringUUIDGenerator) EncodeValue() {
	s := uuid.NewString()

	g.buffer.Write(int32Bytes(int32(len(s) + 1)))
	g.buffer.Write([]byte(s))
	g.buffer.WriteSingleByte(byte(0))
}

func (g *stringUUIDGenerator) EncodeValueAsString() {
	g.buffer.Write([]byte(uuid.NewString()))
}
