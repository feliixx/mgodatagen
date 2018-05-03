// Package generators used to create bson encoded random data.
//
// Relevant documentation:
//
//     http://bsonspec.org/#/specification
//
//
// It was created as part of mgodatagen, but is standalone
// and may be used on its own.
package generators

import (
	"crypto/md5"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/MichaelTJones/pcg"
	"github.com/globalsign/mgo/bson"
	"github.com/manveru/faker"
)

var (
	// stores value for ref fields
	mapRef = map[int][][]byte{}
	// stores bson type for each ref array
	mapRefType = map[int]byte{}
	// machine ID to generate unique object ID
	machineID = readMachineID()
	// process ID to generate unique object ID
	processID = os.Getpid()
	// objectIdCounter is atomically incremented when generating a new ObjectId
	objectIDCounter = getRandomUint32()
	// precomputed index. Most of the array
	// will be of short length, so precompute
	// the first indexes values to avoid calls
	// to strconv.Itoa
	// Use array as access is faster than with map
	indexesBytes = [10]byte{
		byte(48),
		byte(49),
		byte(50),
		byte(51),
		byte(52),
		byte(53),
		byte(54),
		byte(55),
		byte(56),
		byte(57),
	}
)

// readMachineId generates and returns a machine id.
// If this function fails to get the hostname it will cause a runtime error.
func readMachineID() []byte {
	var sum [3]byte
	id := sum[:]
	hostname, err1 := os.Hostname()
	if err1 != nil {
		return uint32Bytes(getRandomUint32())[0:3]
	}
	hw := md5.New()
	hw.Write([]byte(hostname))
	copy(id, hw.Sum(nil))
	return id
}

func getRandomUint32() uint32 {
	seed := uint64(time.Now().Unix())
	pcg32 := pcg.NewPCG32().Seed(seed, seed)
	return pcg32.Random()
}

// ClearRef empty references map
func ClearRef() {
	mapRef = make(map[int][][]byte, 0)
	mapRefType = make(map[int]byte, 0)
}

// Generator interface for all generator objects
type Generator interface {
	// Key return the generator key folowed by 0x00
	Key() []byte
	// Type return the bson type of the element as defined
	// in bson spec: http://bsonspec.org/
	Type() byte
	// Value encode a bson element and append it to the generator
	// encoder
	Value()
	// Exists returns true if the generation should be performed.
	Exists() bool
}

// base implements Key(), Type() and Exists() methods. Intended to be
// embeded in each generator
type base struct {
	// []byte(key) + OxOO
	key []byte
	// probability that the element doesn't exist
	nullPercentage uint32
	bsonType       byte
	buffer         *Encoder
	pcg32          *pcg.PCG32
}

// newBase returns a new base
func newBase(key string, nullPercentage uint32, bsonType byte, out *Encoder, pcg32 *pcg.PCG32) base {
	return base{
		key:            append([]byte(key), byte(0)),
		nullPercentage: nullPercentage,
		bsonType:       bsonType,
		buffer:         out,
		pcg32:          pcg32,
	}
}

func (b *base) Key() []byte { return b.key }

// if a generator has a nullPercentage of 10%, this method will return
// true ~90% of the time, and false ~10% of the time
func (b *base) Exists() bool {
	if b.nullPercentage == 0 {
		return true
	}
	// get the last 10 bits of a random int32 to get a number between 0 and 1023,
	// and compare it to nullPercentage * 10
	return b.pcg32.Random()>>22 >= b.nullPercentage
}

// Type return the bson type of the element created by the generator
func (b *base) Type() byte { return b.bsonType }

// Generator for creating random string of a length within [`MinLength`, `MaxLength`]
type stringGenerator struct {
	base
	minLength uint32
	maxLength uint32
}

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-_"
	letterIdxBits = 6                    // 6 bits to represent a letter index (2^6 => 0-63)
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func (g *stringGenerator) Value() {
	length := g.minLength
	if g.minLength != g.maxLength {
		length = g.pcg32.Bounded(g.maxLength-g.minLength+1) + g.minLength
	}
	// +1 for terminating byte 0x00
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

// Generator for creating random int32 between `Min` and `Max`
type int32Generator struct {
	base
	min int32
	max int32
}

func (g *int32Generator) Value() {
	g.buffer.Write(int32Bytes(int32(g.pcg32.Bounded(uint32(g.max-g.min))) + g.min))
}

// Generator for creating random int64 between `Min` and `Max`
type int64Generator struct {
	base
	min   int64
	max   int64
	pcg64 *pcg.PCG64
}

func (g *int64Generator) Value() {
	g.buffer.Write(int64Bytes(int64(g.pcg64.Bounded(uint64(g.max-g.min))) + g.min))
}

// Generator for creating random float64 between `Min` and `Max`
type float64Generator struct {
	base
	mean   float64
	stdDev float64
	pcg64  *pcg.PCG64
}

func (g *float64Generator) Value() {
	g.buffer.Write(float64Bytes((float64(g.pcg64.Random())/(1<<64))*g.stdDev + g.mean))
}

// Generator for creating random decimal128
type decimal128Generator struct {
	base
	pcg64 *pcg.PCG64
}

func (g *decimal128Generator) Value() {
	b := uint64Bytes(g.pcg64.Random())
	g.buffer.Write(b)
	g.buffer.Write(b)
}

// Generator for creating random bool
type boolGenerator struct {
	base
}

func (g *boolGenerator) Value() {
	g.buffer.WriteSingleByte(byte(g.pcg32.Random() & 0x01))
}

// Generator for creating bson.ObjectId
type objectIDGenerator struct {
	base
}

// Value add a bson.ObjectId to the encoder.
func (g *objectIDGenerator) Value() {
	t := uint32(time.Now().Unix())
	i := atomic.AddUint32(&objectIDCounter, 1)
	g.buffer.Write(
		[]byte{
			byte(t >> 24),
			byte(t >> 16),
			byte(t >> 8),
			byte(t),
			machineID[0], // Machine, first 3 bytes of md5(hostname)
			machineID[1],
			machineID[2],
			byte(processID >> 8), // Pid, 2 bytes, specs don't specify endianness, but we use big endian.
			byte(processID),
			byte(i >> 16), // Increment, 3 bytes, big endian
			byte(i >> 8),
			byte(i),
		},
	)
}

// DocumentGenerator for creating random object
type DocumentGenerator struct {
	base
	generators []Generator
}

// Value create a new bson documents from Generators of g. Documents
// bytes are written to the associated Encoder
func (g *DocumentGenerator) Value() {
	g.buffer.Truncate(4)
	for _, gen := range g.generators {
		if gen.Exists() {
			if gen.Type() != bson.ElementNil {
				g.buffer.WriteSingleByte(gen.Type())
				g.buffer.Write(gen.Key())
			}
			gen.Value()
		}
	}
	g.buffer.WriteSingleByte(byte(0))
	g.buffer.WriteAt(0, int32Bytes(int32(g.buffer.Len())))
}

// Add append a new Generator to the DocumentGenerator
func (g *DocumentGenerator) Add(generator Generator) {
	if generator != nil {
		g.generators = append(g.generators, generator)
	}
}

// Generator for creating embedded documents
type embeddedObjectGenerator DocumentGenerator

func (g *embeddedObjectGenerator) Value() {
	current := g.buffer.Len()
	g.buffer.Reserve()
	for _, gen := range g.generators {
		if gen.Exists() {
			if gen.Type() != bson.ElementNil {
				g.buffer.WriteSingleByte(gen.Type())
				g.buffer.Write(gen.Key())
			}
			gen.Value()
		}
	}
	g.buffer.WriteSingleByte(byte(0))
	g.buffer.WriteAt(current, int32Bytes(int32(g.buffer.Len()-current)))
}

// Generator for creating random array
type arrayGenerator struct {
	base
	size      int
	generator Generator
}

func (g *arrayGenerator) Value() {
	current := g.buffer.Len()
	g.buffer.Reserve()
	// array looks like this:
	// size (byte(index) byte(0) value)... byte(0)
	// where index is a string: ["1", "2", "3"...]
	for i := 0; i < g.size; i++ {
		g.buffer.WriteSingleByte(g.generator.Type())
		if i < 10 {
			g.buffer.WriteSingleByte(indexesBytes[i])
		} else {
			g.buffer.Write([]byte(strconv.Itoa(i)))
		}
		g.buffer.WriteSingleByte(byte(0))
		g.generator.Value()
	}
	g.buffer.WriteSingleByte(byte(0))
	g.buffer.WriteAt(current, int32Bytes(int32(g.buffer.Len()-current)))
}

// Generator for creating random binary data
type binaryDataGenerator struct {
	base
	minLength uint32
	maxLength uint32
}

func (g *binaryDataGenerator) Value() {
	length := g.minLength
	if g.minLength != g.maxLength {
		length = g.pcg32.Bounded(g.maxLength-g.minLength+1) + g.minLength
	}
	g.buffer.Write(uint32Bytes(length))
	g.buffer.WriteSingleByte(bson.BinaryGeneric)
	end := 4
	for count := 0; count < int(length); count += 4 {
		b := uint32Bytes(g.pcg32.Random())
		if int(length)-count < 4 {
			end = int(length) - count
		}
		g.buffer.Write(b[0:end])
	}
}

// Generator for creatingrandom date within bounds
type dateGenerator struct {
	base
	startDate uint64
	delta     uint64
	pcg64     *pcg.PCG64
}

func (g *dateGenerator) Value() {
	// dates are not evenly distributed
	g.buffer.Write(uint64Bytes((g.pcg64.Bounded(g.delta) + g.startDate) * 1000))
}

// Generator for creating random GPS coordinates
type positionGenerator struct {
	base
	pcg64 *pcg.PCG64
}

func (g *positionGenerator) Value() {
	current := g.buffer.Len()
	g.buffer.Reserve()
	for i := 0; i < 2; i++ {
		g.buffer.WriteSingleByte(bson.ElementFloat64)
		g.buffer.WriteSingleByte(indexesBytes[i])
		g.buffer.WriteSingleByte(byte(0))
		// 90*(i+1)(2*[0,1] - 1) ==> pos[0] in [-90, 90], pos[1] in [-180, 180]
		g.buffer.Write(float64Bytes(90 * float64(i+1) * (2*(float64(g.pcg64.Random())/(1<<64)) - 1)))
	}
	g.buffer.WriteSingleByte(byte(0))
	g.buffer.WriteAt(current, int32Bytes(int32(g.buffer.Len()-current)))
}

// ConstGenerator for creating constant value. Val already contains the bson element
// type and the key in addition of the actual value
type constGenerator struct {
	base
	val []byte
}

func (g *constGenerator) Value() {
	g.buffer.Write(g.val)
}

// Generator for creating auto-incremented int32
type autoIncrementGenerator32 struct {
	base
	counter int32
}

func (g *autoIncrementGenerator32) Value() {
	g.buffer.Write(int32Bytes(g.counter))
	g.counter++
}

// Generator for creating auto-incremented int64
type autoIncrementGenerator64 struct {
	base
	counter int64
}

func (g *autoIncrementGenerator64) Value() {
	g.buffer.Write(int64Bytes(g.counter))
	g.counter++
}

// Generator for creating a random value from an array of user-defined values
type fromArrayGenerator struct {
	base
	size          int
	array         [][]byte
	index         int
	doNotTruncate bool
}

func (g *fromArrayGenerator) Value() {
	if g.index == g.size {
		g.index = 0
	}
	g.buffer.Write(g.array[g.index])
	g.index++
}

// Generator for creating random string using faker library
type fakerGenerator struct {
	base
	faker *faker.Faker
	f     func(f *faker.Faker) string
}

func (g *fakerGenerator) Value() {
	fakerVal := []byte(g.f(g.faker))
	g.buffer.Write(int32Bytes(int32(len(fakerVal) + 1)))
	g.buffer.Write(fakerVal)
	g.buffer.WriteSingleByte(byte(0))
}
