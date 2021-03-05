package generators

import (
	"errors"
	"fmt"
)

type stringFromPartGenerator struct {
	base
	parts []Generator
}

func newStringFromPartsGenerator(config *Config, base base, ci *CollInfo, buffer *DocBuffer) (Generator, error) {
	if len(config.Parts) == 0 {
		return nil, errors.New("'parts' can't be null or empty")
	}

	parts := make([]Generator, 0, len(config.Parts))
	for _, c := range config.Parts {
		g, err := ci.newGenerator(buffer, "", &c)
		if err != nil {
			return nil, fmt.Errorf("invalid parts generator: %v", err)
		}
		parts = append(parts, g)
	}

	return &stringFromPartGenerator{
		base:  base,
		parts: parts,
	}, nil
}

func (g *stringFromPartGenerator) Value() {

	start := g.buffer.Len()
	g.buffer.Reserve()
	length := uint32(0)

	for _, p := range g.parts {
		s := p.String()
		if len(s) == 0 {
			continue
		}
		length += uint32(len(s))
		g.buffer.Write([]byte(s))
	}
	g.buffer.WriteSingleByte(0)

	g.buffer.WriteAt(start, uint32Bytes(length+1))
}
