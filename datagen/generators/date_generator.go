package generators

import (
	"errors"
	"math/rand/v2"
	"time"
)

// Generator for creating random date within bounds
type dateGenerator struct {
	base
	startDate uint64
	delta     uint64
	rand      *rand.Rand
}

func newDateGenerator(config *Config, base base, rand *rand.Rand) (Generator, error) {
	if config.StartDate.Unix() > config.EndDate.Unix() {
		return nil, errors.New("make sure that 'startDate' < 'endDate'")
	}
	return &dateGenerator{
		base:      base,
		startDate: uint64(config.StartDate.Unix()),
		delta:     uint64(config.EndDate.Unix() - config.StartDate.Unix()),
		rand:      rand,
	}, nil
}

func (g *dateGenerator) EncodeValue() {
	// dates are not evenly distributed
	g.buffer.Write(uint64Bytes((g.rand.Uint64N(g.delta) + g.startDate) * 1000))
}

func (g *dateGenerator) EncodeValueAsString() {
	s := (g.rand.Uint64N(g.delta) + g.startDate) * 1000
	t := time.Unix(int64(s), 0)
	g.buffer.WriteString(t.Format(time.RFC822))
}
