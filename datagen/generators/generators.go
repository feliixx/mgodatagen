// Package generators used to create bson encoded random data.
//
// Relevant documentation:
//
//     http://bsonspec.org/#/specification
//
//
// It was created as part of mgodatagen, but may be used on its own.
package generators

import (
	"crypto/md5"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/MichaelTJones/pcg"
	"github.com/manveru/faker"
	uuid "github.com/satori/go.uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// DocumentGenerator is a Generator for creating random bson documents
type DocumentGenerator struct {
	// Buffer holds the document bytes
	Buffer *DocBuffer
	// list of all generators used to create the document. The resulting document
	// will have n keys where 0 < n < len(Generators)
	Generators []Generator
}

// Generate creates a new bson document and returns it as a slice of bytes
func (g *DocumentGenerator) Generate() []byte {
	g.Buffer.Truncate(4)
	for _, gen := range g.Generators {
		if gen.Exists() {
			if gen.Type() != bson.TypeNull {
				g.Buffer.WriteSingleByte(byte(gen.Type()))
				g.Buffer.Write(gen.Key())
				g.Buffer.WriteSingleByte(byte(0))
			}
			gen.Value()
		}
	}
	g.Buffer.WriteSingleByte(byte(0))
	g.Buffer.WriteAt(0, int32Bytes(int32(g.Buffer.Len())))
	return g.Buffer.Bytes()
}

// Add append a new Generator to the DocumentGenerator. The generator Value() method
// must write to the same DocBuffer as the DocumentGenerator g
func (g *DocumentGenerator) Add(generator Generator) {
	if generator != nil {
		g.Generators = append(g.Generators, generator)
	}
}

// Generator is an interface for generator that can be used to
// generate random value of a specific type, and encode them in bson
// format
type Generator interface {
	// Key returns the element key
	Key() []byte
	// Type returns the bson type of the element as defined in bson spec: http://bsonspec.org/
	Type() bsontype.Type
	// Value encodes a random value in bson and write it to a DocBuffer
	Value()
	// Exists returns true if the generation should be performed.
	Exists() bool
}

// base implements Key(), Type() and Exists() methods. Intended to be
// embedded in each generator
type base struct {
	key []byte
	// probability that the element doesn't exist
	nullPercentage uint32
	bsonType       bsontype.Type
	buffer         *DocBuffer
	pcg32          *pcg.PCG32
}

// newBase returns a new base
func newBase(key string, nullPercentage uint32, bsonType bsontype.Type, out *DocBuffer, pcg32 *pcg.PCG32) base {
	return base{
		key:            []byte(key),
		nullPercentage: nullPercentage,
		bsonType:       bsonType,
		buffer:         out,
		pcg32:          pcg32,
	}
}

func (g *base) Key() []byte { return g.key }

// if a generator has a nullPercentage of 10%, this method will return
// true ~90% of the time, and false ~10% of the time
func (g *base) Exists() bool {
	if g.nullPercentage == 0 {
		return true
	}
	// get the last 10 bits of a random int32 to get a number between 0 and 1023,
	// and compare it to nullPercentage * 10
	return g.pcg32.Random()>>22 >= g.nullPercentage
}

// Type return the bson type of the element created by the generator
func (g *base) Type() bsontype.Type { return g.bsonType }

// Generator for creating random string of a length within [`MinLength`, `MaxLength`]
type stringGenerator struct {
	base
	minLength uint32
	maxLength uint32
}

// following code is an adaptation of existing code from this question:
// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang/
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

// readMachineId generates and returns a machine id.
func readMachineID() []byte {
	id := uint32Bytes(getRandomUint32())
	hostname, err := os.Hostname()
	if err == nil {
		h := md5.New()
		h.Write([]byte(hostname))
		id = h.Sum(nil)
	}
	return id[0:3]
}

func getRandomUint32() uint32 {
	seed := uint64(time.Now().Unix())
	pcg32 := pcg.NewPCG32().Seed(seed, seed)
	return pcg32.Random()
}

var (
	// machine ID to generate unique object ID
	machineID = readMachineID()
	// process ID to generate unique object ID
	processID = os.Getpid()
	// objectIdCounter is atomically incremented when generating a new ObjectId
	objectIDCounter = getRandomUint32()
)

// Generator for creating bson.ObjectId
type objectIDGenerator struct {
	base
}

// Value add a bson.ObjectId to the DocBuffer.
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

// Generator for creating embedded documents
type embeddedObjectGenerator struct {
	base
	generators []Generator
}

func (g *embeddedObjectGenerator) Value() {
	current := g.buffer.Len()
	g.buffer.Reserve()
	for _, gen := range g.generators {
		if gen.Exists() {
			if gen.Type() != bson.TypeNull {
				g.buffer.WriteSingleByte(byte(gen.Type()))
				g.buffer.Write(gen.Key())
				g.buffer.WriteSingleByte(byte(0))
			}
			gen.Value()
		}
	}
	g.buffer.WriteSingleByte(byte(0))
	g.buffer.WriteAt(current, int32Bytes(int32(g.buffer.Len()-current)))
}

// precomputed index. Most of the array
// will be of short length, so precompute
// the first indexes values to avoid calls
// to strconv.Itoa
// Use array as access is faster than with map
var indexesBytes = [10]byte{
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
		g.buffer.WriteSingleByte(byte(g.generator.Type()))
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

// legacy type binary instead of 0x05
const genericBinaryType = 0x00

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
	g.buffer.WriteSingleByte(genericBinaryType)
	end := 4
	for count := 0; count < int(length); count += 4 {
		b := uint32Bytes(g.pcg32.Random())
		if int(length)-count < 4 {
			end = int(length) - count
		}
		g.buffer.Write(b[0:end])
	}
}

// Generator for creating random date within bounds
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
		g.buffer.WriteSingleByte(byte(bson.TypeDouble))
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
	randomOrder   bool
	doNotTruncate bool
}

func (g *fromArrayGenerator) Value() {

	if g.randomOrder {
		g.index = int(g.base.pcg32.Bounded(uint32(g.size)))
	} else if g.index == g.size {
		g.index = 0
	}

	g.buffer.Write(g.array[g.index])
	g.index++
}

type uuidGenerator struct {
	base
}

func (g *uuidGenerator) Value() {
	uuid, _ := uuid.NewV4()
	strUUID := uuid.String()

	g.buffer.Write(int32Bytes(int32(len(strUUID) + 1)))
	g.buffer.Write([]byte(strUUID))
	g.buffer.WriteSingleByte(byte(0))
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
