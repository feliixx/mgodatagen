package generators

import (
	uuid "github.com/satori/go.uuid"
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
	uuid, _ := uuid.NewV4()
	b := uuid.Bytes()

	g.buffer.Write(int32Bytes(int32(len(b))))
	g.buffer.WriteSingleByte(binaryUUIDSubtype)
	g.buffer.Write(b)
}

func (g *binaryUUIDGenerator) EncodeValueAsString() {
	uuid, _ := uuid.NewV4()
	g.buffer.Write([]byte(uuid.String()))
}
