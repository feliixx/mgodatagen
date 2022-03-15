package generators

import (
	"github.com/MichaelTJones/pcg"
	"go.mongodb.org/mongo-driver/bson"
)

// Generator for creating random GPS coordinates
type positionGenerator struct {
	base
	pcg64 *pcg.PCG64
}

func newPositionGenerator(base base, pcg64 *pcg.PCG64) (Generator, error) {
	return &positionGenerator{
		base:  base,
		pcg64: pcg64,
	}, nil
}

func (g *positionGenerator) EncodeValue() {
	current := g.buffer.Len()
	g.buffer.Reserve()

	// longitude, in [-180, 180]
	g.buffer.WriteSingleByte(byte(bson.TypeDouble))
	g.buffer.WriteSingleByte(indexesBytes[0])
	g.buffer.WriteSingleByte(byte(0))
	g.buffer.Write(float64Bytes(180 * (2*(float64(g.pcg64.Random())/(1<<64)) - 1)))

	// latitude, in [-90, 90]
	g.buffer.WriteSingleByte(byte(bson.TypeDouble))
	g.buffer.WriteSingleByte(indexesBytes[1])
	g.buffer.WriteSingleByte(byte(0))
	g.buffer.Write(float64Bytes(90 * (2*(float64(g.pcg64.Random())/(1<<64)) - 1)))

	g.buffer.WriteSingleByte(byte(0))
	g.buffer.WriteAt(current, int32Bytes(int32(g.buffer.Len()-current)))
}
