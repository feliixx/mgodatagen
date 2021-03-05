package generators

import "go.mongodb.org/mongo-driver/bson"

// Generator for creating embedded documents
type embeddedObjectGenerator struct {
	base
	generators []Generator
}

func newEmbededGenerator(config *Config, base base, ci *CollInfo, buffer *DocBuffer) (Generator, error) {
	emg := &embeddedObjectGenerator{
		base:       base,
		generators: make([]Generator, 0, len(config.ObjectContent)),
	}
	for k, v := range config.ObjectContent {
		g, err := ci.newGenerator(buffer, k, &v)
		if err != nil {
			return nil, err
		}
		if g != nil {
			emg.generators = append(emg.generators, g)
		}
	}
	return emg, nil
}

func (g *embeddedObjectGenerator) EncodeValue() {
	current := g.buffer.Len()
	g.buffer.Reserve()
	for _, gen := range g.generators {
		if gen.Exists() {
			if gen.Type() != bson.TypeNull {
				g.buffer.WriteSingleByte(byte(gen.Type()))
				g.buffer.Write(gen.Key())
				g.buffer.WriteSingleByte(byte(0))
			}
			gen.EncodeValue()
		}
	}
	g.buffer.WriteSingleByte(byte(0))
	g.buffer.WriteAt(current, int32Bytes(int32(g.buffer.Len()-current)))
}
