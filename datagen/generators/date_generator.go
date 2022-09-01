package generators

import (
	"errors"
	"time"

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

func (g *dateGenerator) EncodeValueAsString() {
	s := (g.pcg64.Bounded(g.delta) + g.startDate) * 1000
	t := time.Unix(int64(s), 0)
	g.buffer.Write([]byte(t.Format(time.RFC822)))
}
