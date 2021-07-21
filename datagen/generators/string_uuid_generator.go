package generators

import (
	uuid "github.com/satori/go.uuid"
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
	uuid, _ := uuid.NewV4()
	s := uuid.String()

	g.buffer.Write(int32Bytes(int32(len(s) + 1)))
	g.buffer.Write([]byte(s))
	g.buffer.WriteSingleByte(byte(0))
}

func (g *stringUUIDGenerator) EncodeValueAsString() {
	uuid, _ := uuid.NewV4()
	g.buffer.Write([]byte(uuid.String()))
}
