package generators

import uuid "github.com/satori/go.uuid"

type uuidGenerator struct {
	base
}

func newUUIDGenerator(base base) (Generator, error) {
	return &uuidGenerator{base: base}, nil
}

func (g *uuidGenerator) EncodeValue() {
	uuid, _ := uuid.NewV4()
	strUUID := uuid.String()

	g.buffer.Write(int32Bytes(int32(len(strUUID) + 1)))
	g.buffer.Write([]byte(strUUID))
	g.buffer.WriteSingleByte(byte(0))
}

func (g *uuidGenerator) EncodeValueAsString() {
	uuid, _ := uuid.NewV4()
	g.buffer.Write([]byte(uuid.String()))
}
