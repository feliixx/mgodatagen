package generators

import (
	"errors"

	"github.com/MichaelTJones/pcg"
)

// Generator for creating random date within bounds
type dateGenerator struct {
	base
	startDate uint64
	delta     uint64
	pcg64     *pcg.PCG64
}

func newDateGenerator(config *Config, base base, pcg64 *pcg.PCG64) (Generator, error) {
	if config.StartDate.Unix() > config.EndDate.Unix() {
		return nil, errors.New("make sure that 'startDate' < 'endDate'")
	}
	return &dateGenerator{
		base:      base,
		startDate: uint64(config.StartDate.Unix()),
		delta:     uint64(config.EndDate.Unix() - config.StartDate.Unix()),
		pcg64:     pcg64,
	}, nil
}

func (g *dateGenerator) EncodeValue() {
	// dates are not evenly distributed
	g.buffer.Write(uint64Bytes((g.pcg64.Bounded(g.delta) + g.startDate) * 1000))
}
