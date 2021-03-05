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
	for i := 0; i < 2; i++ {
		g.buffer.WriteSingleByte(byte(bson.TypeDouble))
		g.buffer.WriteSingleByte(indexesBytes[i])
		g.buffer.WriteSingleByte(byte(0))
		// 90*(i+1)(2*[0,1] - 1) ==> pos[0] in [-90, 90], pos[1] in [-180, 180]
		g.buffer.Write(float64Bytes(90 * float64(i+1) * (2*(float64(g.pcg64.Random())/(1<<64)) - 1)))
	}
	g.buffer.WriteSingleByte(byte(0))
	g.buffer.WriteAt(current, int32Bytes(int32(g.buffer.Len()-current)))
}
