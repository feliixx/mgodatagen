package generators

import (
	"math/rand/v2"
	"strconv"

	"go.mongodb.org/mongo-driver/bson"
)

// Generator for creating random GPS coordinates
type positionGenerator struct {
	base
	rand *rand.Rand
}

func newPositionGenerator(base base, rand *rand.Rand) (Generator, error) {
	return &positionGenerator{
		base: base,
		rand: rand,
	}, nil
}

func (g *positionGenerator) EncodeValue() {
	current := g.buffer.Len()
	g.buffer.Reserve()

	// longitude, in [-180, 180]
	g.buffer.WriteSingleByte(byte(bson.TypeDouble))
	g.buffer.WriteSingleByte(indexesBytes[0])
	g.buffer.WriteSingleByte(byte(0))
	g.buffer.Write(float64Bytes(180 * (2*g.rand.Float64() - 1)))

	// latitude, in [-90, 90]
	g.buffer.WriteSingleByte(byte(bson.TypeDouble))
	g.buffer.WriteSingleByte(indexesBytes[1])
	g.buffer.WriteSingleByte(byte(0))
	g.buffer.Write(float64Bytes(90 * (2*g.rand.Float64() - 1)))

	g.buffer.WriteSingleByte(byte(0))
	g.buffer.WriteAt(current, int32Bytes(int32(g.buffer.Len()-current)))
}

func (g *positionGenerator) EncodeValueAsString() {

	longitude := 180 * (2*g.rand.Float64() - 1)
	latitude := 90 * (2*g.rand.Float64() - 1)

	g.buffer.WriteSingleByte('[')
	g.buffer.WriteString(strconv.FormatFloat(longitude, 'f', 10, 64))
	g.buffer.WriteSingleByte(',')
	g.buffer.WriteString(strconv.FormatFloat(latitude, 'f', 10, 64))
	g.buffer.WriteSingleByte(']')
}
