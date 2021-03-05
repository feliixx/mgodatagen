package generators

import "fmt"

// Generator for creating random string using faker library
type fakerGenerator struct {
	base
	f func() string
}

func newFakerGenerator(config *Config, base base) (Generator, error) {
	method, ok := fakerMethods[config.Method]
	if !ok {
		return nil, fmt.Errorf("invalid Faker method '%s'", config.Method)
	}
	return &fakerGenerator{
		base: base,
		f:    method,
	}, nil
}

func (g *fakerGenerator) EncodeValue() {
	fakerVal := []byte(g.f())
	g.buffer.Write(int32Bytes(int32(len(fakerVal) + 1)))
	g.buffer.Write(fakerVal)
	g.buffer.WriteSingleByte(byte(0))
}

func (g *fakerGenerator) EncodeValueAsString() {
	g.buffer.Write([]byte(g.f()))
}
