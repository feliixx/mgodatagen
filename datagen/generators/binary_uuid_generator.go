package generators

import (
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
)

type binaryUUIDGenerator struct {
	base
}

func newBinaryUUIDGenerator(base base) (Generator, error) {
	base.bsonType = bson.TypeBinary
	return &binaryUUIDGenerator{base: base}, nil
}

// see https://bsonspec.org/spec.html
const binaryUUIDSubtype = 0x04

func (g *binaryUUIDGenerator) EncodeValue() {
	b, _ := uuid.New().MarshalBinary()

	g.buffer.Write(int32Bytes(int32(len(b))))
	g.buffer.WriteSingleByte(binaryUUIDSubtype)
	g.buffer.Write(b)
}

func (g *binaryUUIDGenerator) EncodeValueAsString() {
	g.buffer.Write([]byte(uuid.NewString()))
}
