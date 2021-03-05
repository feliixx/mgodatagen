package generators

import (
	"errors"
	"fmt"
	"math"
)

// Generator for creating random string of a length within [`MinLength`, `MaxLength`]
type stringGenerator struct {
	base
	minLength uint32
	maxLength uint32
}

func newStringGenerator(config *Config, base base, nbDoc int) (Generator, error) {
	if config.MinLength < 0 || config.MinLength > config.MaxLength {
		return nil, errors.New("make sure that 'minLength' >= 0 and 'minLength' <= 'maxLength'")
	}
	if config.Unique {
		values, err := uniqueValues(nbDoc, config.MaxLength)
		if err != nil {
			return nil, err
		}
		return newFromArrayGeneratorWithPregeneratedValues(base, values, false)
	}
	return &stringGenerator{
		base:      base,
		minLength: uint32(config.MinLength),
		maxLength: uint32(config.MaxLength),
	}, nil
}

// following code is an adaptation of existing code from this question:
// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang/
const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-_"
	letterIdxBits = 6                    // 6 bits to represent a letter index (2^6 => 0-63)
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func (g *stringGenerator) EncodeValue() {
	length := g.minLength
	if g.minLength != g.maxLength {
		length = g.pcg32.Bounded(g.maxLength-g.minLength+1) + g.minLength
	}
	g.buffer.Write(uint32Bytes(length + 1))
	cache, remain := g.pcg32.Random(), letterIdxMax
	for i := 0; i < int(length); i++ {
		if remain == 0 {
			cache, remain = g.pcg32.Random(), letterIdxMax
		}
		g.buffer.WriteSingleByte(letterBytes[cache&letterIdxMask])
		cache >>= letterIdxBits
		remain--
	}
	g.buffer.WriteSingleByte(byte(0))
}

type unique struct {
	values       [][]byte
	currentIndex int
}

// recursively generate all possible combinations with repeat
func (u *unique) recur(data []byte, stringSize int, index int, docCount int) {
	for i := 0; i < len(letterBytes); i++ {
		if u.currentIndex < docCount {
			data[index+4] = letterBytes[i]
			if index == stringSize-1 {
				tmp := make([]byte, len(data))
				copy(tmp, data)
				u.values[u.currentIndex] = tmp
				u.currentIndex++
			} else {
				u.recur(data, stringSize, index+1, docCount)
			}
		}
	}
}

// generate an array of length 'docCount' containing unique string
// array will look like (for stringSize=3)
// [ "aaa", "aab", "aac", ...]
func uniqueValues(docCount int, stringSize int) ([][]byte, error) {
	if stringSize == 0 {
		return nil, fmt.Errorf("with unique generator, MinLength has to be > 0")
	}
	// if string size >= 5, there is at least 1073741824 possible string, so don't bother checking collection count
	if stringSize < 5 {
		maxNumber := int(math.Pow(float64(len(letterBytes)), float64(stringSize)))
		if docCount > maxNumber {
			return nil, fmt.Errorf("doc count is greater than possible value for string of size %d, max is %vd( %d^%d) ", stringSize, maxNumber, len(letterBytes), stringSize)
		}
	}
	u := &unique{
		values:       make([][]byte, docCount),
		currentIndex: 0,
	}
	data := make([]byte, stringSize+5)
	copy(data[0:4], int32Bytes(int32(stringSize)+1))

	u.recur(data, stringSize, 0, docCount)
	return u.values, nil
}
