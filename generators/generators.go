// Package generators used to create bson objects
// Relevant documentation:
//
//     http://bsonspec.org/#/specification
//
// Currently supported BSON types:
//  - string
//  - int
//  - long
//  - double
//  - boolean
//  - date
//  - objectId
//  - object
//  - array
//  - binary data
//
// Custom types :
//  - GPS position
//  - constant
//  - autoincrement
//  - reference
//  - from array
//  - faker (cf https://github.com/manveru/faker)
//
// It was created as part of mgodatagen, but is standalone
// and may be used on its own.
package generators

import (
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/MichaelTJones/pcg"
	"github.com/globalsign/mgo/bson"
	"github.com/manveru/faker"

	"github.com/feliixx/mgodatagen/config"
)

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-_"
	letterIdxBits = 6                    // 6 bits to represent a letter index (2^6 => 0-63)
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	// CountAggregator count mode
	CountAggregator = 0
	// ValueAggregator value mode
	ValueAggregator = 1
	// BoundAggregator bound mode
	BoundAggregator = 2
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
		_, err2 := io.ReadFull(rand.Reader, id)
		if err2 != nil {
			panic(fmt.Errorf("cannot get hostname: %v; %v", err1, err2))
		}
		return id
	}
	hw := md5.New()
	hw.Write([]byte(hostname))
	copy(id, hw.Sum(nil))
	return id
}

func getRandomUint32() uint32 {
	var b [4]byte
	_, err := io.ReadFull(rand.Reader, b[:])
	if err != nil {
		panic(fmt.Errorf("cannot read random object id: %v", err))
	}
	return uint32((uint32(b[0]) << 0) | (uint32(b[1]) << 8) | (uint32(b[2]) << 16) | (uint32(b[3]) << 24))
}

// ClearRef empty references map for tests run
func ClearRef() {
	mapRef = make(map[int][][]byte, 0)
	mapRefType = make(map[int]byte, 0)
}

// Int32Bytes convert an int32 into an array of bytes
func Int32Bytes(v int32) []byte {
	u := uint32(v)
	return UInt32Bytes(u)
}

// UInt32Bytes returns an uint32 into an array of bytes
func UInt32Bytes(v uint32) []byte {
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
}

// Int64Bytes convert an int64 into an array of bytes
func Int64Bytes(v int64) []byte {
	u := uint64(v)
	return UInt64Bytes(u)
}

// UInt64Bytes returns an uint64 into an array of bytes
func UInt64Bytes(v uint64) []byte {
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24),
		byte(v >> 32), byte(v >> 40), byte(v >> 48), byte(v >> 56)}
}

// Float64Bytes convert an int32 into an array of bytes
func Float64Bytes(v float64) []byte {
	return Int64Bytes(int64(math.Float64bits(v)))
}

// CollInfo stores global info on the colelction to generate
type CollInfo struct {
	Count      int
	ShortNames bool
	Version    []int
	Encoder    *Encoder
}

// Generator interface for all generator objects
type Generator interface {
	Key() []byte
	Type() byte
	Value()
	Exists() bool
	SetEncoder(e *Encoder)
}

// EmptyGenerator serves as base for the actual generators
type EmptyGenerator struct {
	// []byte(key) + OxOO
	K []byte
	// probability that the element is null, ie doesn't exist (nullPercentage/1000)
	NullPercentage uint32
	// bson type as defined here: http://bsonspec.org/
	T byte
	// structure to hold the encoded document
	Out *Encoder
}

// Encoder holds the encoded bytes of the generated document, and a random
// source. Use github.com/MichaelTJones/pcg instead of math/rand as it's
// way faster
type Encoder struct {
	Data  []byte
	PCG32 *pcg.PCG32
	PCG64 *pcg.PCG64
}

// NewEncoder returns a new encoder with PCG seeded with
// time.Now()
func NewEncoder(size int) *Encoder {
	now := uint64(time.Now().Unix())
	return &Encoder{
		Data:  make([]byte, size),
		PCG32: pcg.NewPCG32().Seed(now, now),
		PCG64: pcg.NewPCG64().Seed(now, now, now, now),
	}
}

// Truncate discards all but the first n unread bytes from the buffer
func (e *Encoder) Truncate(n int) {
	e.Data = e.Data[0:n]
}

// Write appends bytes to the buffer
func (e *Encoder) Write(b []byte) {
	e.Data = append(e.Data, b...)
}

// WriteSingleByte appends a single byte to the buffer
func (e *Encoder) WriteSingleByte(b byte) {
	e.Data = append(e.Data, b)
}

// WriteAt writes bytes to the buffer at a specific
// position
func (e *Encoder) WriteAt(startPos int, b []byte) {
	copy(e.Data[startPos:startPos+len(b)], b)
}

// Reserve add 4 bytes to the buffer. Thoses bytes will be set
// once the bson value size is known
func (e *Encoder) Reserve() {
	e.Data = append(e.Data, byte(0), byte(0), byte(0), byte(0))
}

// PreGenerate generate `nb`values using a generator created from config
func (ci *CollInfo) PreGenerate(k string, v *config.GeneratorJSON, nb int) ([][]byte, byte, error) {

	g, err := ci.newGenerator(k, v)
	if err != nil {
		return nil, bson.ElementNil, fmt.Errorf("for field %s, error while creating base array: %v", k, err)
	}
	e := NewEncoder(0)
	g.SetEncoder(e)

	arr := make([][]byte, nb)
	for i := 0; i < nb; i++ {
		g.Value()
		tmpArr := make([]byte, len(e.Data))
		copy(tmpArr, e.Data)
		arr[i] = tmpArr
		e.Truncate(0)
	}
	return arr, g.Type(), nil
}

// Key return the key (bson::e_name) encoded in UTF-8, followed by 0x00
func (g *EmptyGenerator) Key() []byte { return g.K }

// Exists returns true if the generation should be performed
// get the last 10 bits of the random int32 to get a number between 0 and 1023,
// and compare it to nullPercentage * 10
func (g *EmptyGenerator) Exists() bool {
	if g.NullPercentage == 0 {
		return true
	}
	return g.Out.PCG32.Random()>>22 >= g.NullPercentage
}

// Type return the bson type of the element created by the generator
func (g *EmptyGenerator) Type() byte { return g.T }

// SetEncoder set the EmptyGenerator encoder
func (g *EmptyGenerator) SetEncoder(e *Encoder) { g.Out = e }

// getLength return a random uint32 between min and max
func (g *EmptyGenerator) getLength(min, max uint32) uint32 {
	if min != max {
		return g.Out.PCG32.Bounded(max-min) + min
	}
	return min
}

// StringGenerator struct that implements Generator. Used to
// generate random string of a specific length in [`MinLength`, `MaxLength`]
type StringGenerator struct {
	EmptyGenerator
	MinLength uint32
	MaxLength uint32
}

// Value add a random String of a specific length to the encoder
func (g *StringGenerator) Value() {
	length := g.getLength(g.MinLength, g.MaxLength)
	// first, put the size of the string, which is length + 1 because of
	// the terminating byte 0x00
	g.Out.Write(UInt32Bytes(length + 1))
	// create the random string
	cache, remain := g.Out.PCG32.Random(), letterIdxMax
	for i := 0; i < int(length); i++ {
		if remain == 0 {
			cache, remain = g.Out.PCG32.Random(), letterIdxMax
		}
		g.Out.WriteSingleByte(letterBytes[cache&letterIdxMask])
		cache >>= letterIdxBits
		remain--
	}
	// end the string
	g.Out.WriteSingleByte(byte(0))
}

// Int32Generator struct that implements Generator. Used to
// generate random int32 between `Min` and `Max`
type Int32Generator struct {
	EmptyGenerator
	Min int32
	Max int32
}

// Value add a random int32 between `g.Min` and `g.Max` to the
// encoder
func (g *Int32Generator) Value() {
	g.Out.Write(Int32Bytes(int32(g.Out.PCG32.Bounded(uint32(g.Max-g.Min))) + g.Min))
}

// Int64Generator struct that implements Generator. Used to
// generate random int64 between `Min` and `Max`
type Int64Generator struct {
	EmptyGenerator
	Min int64
	Max int64
}

// Value add a random int64 between `g.Min` and `g.Max` to the encoder
func (g *Int64Generator) Value() {
	g.Out.Write(Int64Bytes(int64(g.Out.PCG64.Bounded(uint64(g.Max-g.Min))) + g.Min))
}

// Float64Generator struct that implements Generator. Used to
// generate random int64 between `Min` and `Max`
type Float64Generator struct {
	EmptyGenerator
	Mean   float64
	StdDev float64
}

// Value returns a random float64 between `g.Min` and `g.Max`
func (g *Float64Generator) Value() {
	g.Out.Write(Float64Bytes((float64(g.Out.PCG64.Random())/(1<<64))*g.StdDev + g.Mean))
}

// Decimal128Generator struct that implements Generator. Used to
// generate random decimal128
type Decimal128Generator struct {
	EmptyGenerator
}

// Value returns a random Decimal128
func (g *Decimal128Generator) Value() {
	b := UInt64Bytes(g.Out.PCG64.Random())
	g.Out.Write(b)
	g.Out.Write(b)
}

// BoolGenerator struct that implements Generator. Used to
// generate random bool
type BoolGenerator struct {
	EmptyGenerator
}

// Value add a random boolean to the encoder.
func (g *BoolGenerator) Value() {
	g.Out.WriteSingleByte(byte(g.Out.PCG32.Random() & 0x01))
}

// ObjectIDGenerator struct that implements Generator. Used to
// generate bson.ObjectId
type ObjectIDGenerator struct {
	EmptyGenerator
}

// Value add a bson.ObjectId to the encoder. As object generation
// is done in a single goroutine, ne need for sync/atomic
func (g *ObjectIDGenerator) Value() {
	t := uint32(time.Now().Unix())
	i := atomic.AddUint32(&objectIDCounter, 1)
	g.Out.Write(
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

// ObjectGenerator struct that implements Generator. Used to
// generate random object
type ObjectGenerator struct {
	EmptyGenerator
	Generators []Generator
}

// Value add a random object to the encoder
func (g *ObjectGenerator) Value() {
	// reset the buffer. 4 first bytes are used to store
	// the size of the document
	g.Out.Truncate(4)
	// generate each element of the document
	for _, gen := range g.Generators {
		if gen.Exists() {
			if gen.Type() != bson.ElementNil {
				g.Out.WriteSingleByte(gen.Type())
				g.Out.Write(gen.Key())
			}
			gen.Value()
		}
	}
	// end the document
	g.Out.WriteSingleByte(byte(0))
	// set the document size
	g.Out.WriteAt(0, Int32Bytes(int32(len(g.Out.Data))))
}

// EmbeddedObjectGenerator struct that implements Generator. Used to
// generate embedded documents
type EmbeddedObjectGenerator ObjectGenerator

// Value add a random document to the encoder
func (g *EmbeddedObjectGenerator) Value() {
	// keep trace of current position so we can update the size of the
	// document once it's been generated
	current := len(g.Out.Data)
	// reserve 4 bytes to store the size of the embedded document
	g.Out.Reserve()
	for _, gen := range g.Generators {
		if gen.Exists() {
			if gen.Type() != bson.ElementNil {
				g.Out.WriteSingleByte(gen.Type())
				g.Out.Write(gen.Key())
			}
			gen.Value()
		}
	}
	// end sub document
	g.Out.WriteSingleByte(byte(0))
	// update sub document size
	g.Out.WriteAt(current, Int32Bytes(int32(len(g.Out.Data)-current)))
}

// ArrayGenerator struct that implements Generator. Used to
// generate random array
type ArrayGenerator struct {
	EmptyGenerator
	Size      int
	Generator Generator
}

// Value add a random array of `g.size` size to the encoder. It's feed with the
// provided generator
func (g *ArrayGenerator) Value() {
	current := len(g.Out.Data)
	// reserve 4 bytes to write the size of the array
	g.Out.Reserve()
	// array looks like this:
	// size (byte(index) byte(0) value)... byte(0)
	// where index is a string: ["1", "2", "3"...]
	for i := 0; i < g.Size; i++ {
		g.Out.WriteSingleByte(g.Generator.Type())
		if i < 10 {
			g.Out.WriteSingleByte(indexesBytes[i])
		} else {
			g.Out.Write([]byte(strconv.Itoa(i)))
		}
		g.Out.WriteSingleByte(byte(0))
		g.Generator.Value()
	}
	g.Out.WriteSingleByte(byte(0))
	g.Out.WriteAt(current, Int32Bytes(int32(len(g.Out.Data)-current)))
}

// BinaryDataGenerator struct that implements Generator. Used to
// generate random binary data
type BinaryDataGenerator struct {
	EmptyGenerator
	MinLength uint32
	MaxLength uint32
}

// Value add a random array of bytes of length `g.length` to
// the encoder
func (g *BinaryDataGenerator) Value() {
	length := g.getLength(g.MinLength, g.MaxLength)
	g.Out.Write(UInt32Bytes(length))
	g.Out.WriteSingleByte(bson.BinaryGeneric)
	end := 4
	for count := 0; count < int(length); count += 4 {
		b := UInt32Bytes(g.Out.PCG32.Random())
		if int(length)-count < 4 {
			end = int(length) - count
		}
		g.Out.Write(b[0:end])
	}
}

// DateGenerator struct that implements Generator. Used to
// generate random date within bounds
type DateGenerator struct {
	EmptyGenerator
	StartDate uint64
	Delta     uint64
}

// Value add a random date within `g.StartDate` and `g.endDate`
// Date are not evenly distributed
func (g *DateGenerator) Value() {
	g.Out.Write(UInt64Bytes((g.Out.PCG64.Bounded(g.Delta) + g.StartDate) * 1000))
}

// PositionGenerator struct that implements Generator. Used to
// generate random GPS coordinates
type PositionGenerator struct {
	EmptyGenerator
}

// Value add a random position to the encoder.
func (g *PositionGenerator) Value() {
	current := len(g.Out.Data)
	g.Out.Reserve()
	for i := 0; i < 2; i++ {
		g.Out.WriteSingleByte(bson.ElementFloat64)
		g.Out.WriteSingleByte(indexesBytes[i])
		g.Out.WriteSingleByte(byte(0))
		// 90*(i+1)(2*[0,1] - 1) ==> pos[0] in [-90, 90], pos[1] in [-180, 180]
		g.Out.Write(Float64Bytes(90 * float64(i+1) * (2*(float64(g.Out.PCG64.Random())/(1<<64)) - 1)))
	}
	g.Out.WriteSingleByte(byte(0))
	g.Out.WriteAt(current, Int32Bytes(int32(len(g.Out.Data)-current)))
}

// ConstGenerator struct that implements Generator. Used to
// generate constant value. Val already contains the bson element
// type and the key in addition of the actual value
type ConstGenerator struct {
	EmptyGenerator
	Val []byte
}

// Value always add the same value as specified in the config file
func (g *ConstGenerator) Value() {
	g.Out.Write(g.Val)
}

// AutoIncrementGenerator32 struct that implements Generator. Used to
// generate auto-incremented int32
type AutoIncrementGenerator32 struct {
	EmptyGenerator
	Counter int32
}

// Value add prev counter, starting from `g.counter` to the
// encoder
func (g *AutoIncrementGenerator32) Value() {
	g.Out.Write(Int32Bytes(g.Counter))
	g.Counter++
}

// AutoIncrementGenerator64 struct that implements Generator. Used to
// generate auto-incremented int64
type AutoIncrementGenerator64 struct {
	EmptyGenerator
	Counter int64
}

// Value add prev counter, starting from `g.counter` to the
// encoder
func (g *AutoIncrementGenerator64) Value() {
	g.Out.Write(Int64Bytes(g.Counter))
	g.Counter++
}

// FromArrayGenerator struct that implements Generator. Used to
// generate a random value from an array of user-defined values
type FromArrayGenerator struct {
	EmptyGenerator
	Size          int
	Array         [][]byte
	Index         int
	DoNotTruncate bool
}

// Value add an item of the input array to the encoder
func (g *FromArrayGenerator) Value() {
	if g.Index == g.Size {
		g.Index = 0
	}
	g.Out.Write(g.Array[g.Index])
	g.Index++
}

// FakerGenerator struct that implements Generator. Used to
// genrate random string using faker library
type FakerGenerator struct {
	EmptyGenerator
	Faker *faker.Faker
	F     func(f *faker.Faker) string
}

// Value add a string generated by faker library to the
// encoder
func (g *FakerGenerator) Value() {
	fakerVal := []byte(g.F(g.Faker))
	g.Out.Write(Int32Bytes(int32(len(fakerVal) + 1)))
	g.Out.Write(fakerVal)
	g.Out.WriteSingleByte(byte(0))
}

// UniqueGenerator used to create an array containing unique strings
type UniqueGenerator struct {
	Values       [][]byte
	CurrentIndex int
}

// recursively generate all possible combinations with repeat
func (u *UniqueGenerator) recur(data []byte, stringSize int, index int, docCount int) {
	for i := 0; i < len(letterBytes); i++ {
		if u.CurrentIndex < docCount {
			data[index+4] = letterBytes[i]
			if index == stringSize-1 {
				tmp := make([]byte, len(data))
				copy(tmp, data)
				u.Values[u.CurrentIndex] = tmp
				u.CurrentIndex++
			} else {
				u.recur(data, stringSize, index+1, docCount)
			}
		}
	}
}

// generate an array of length 'docCount' containing unique string
// array will look like (for stringSize=3)
// [ "aaa", "aab", "aac", ...]
func (u *UniqueGenerator) getUniqueArray(docCount int, stringSize int) error {
	// if string size >= 5, there is at least 1073741824 possible string, so don't bother checking collection count
	if stringSize < 5 {
		maxNumber := int(math.Pow(float64(len(letterBytes)), float64(stringSize)))
		if docCount > maxNumber {
			return fmt.Errorf("doc count is greater than possible value for string of size %v, max is %v ( %v^%v) ", stringSize, maxNumber, len(letterBytes), stringSize)
		}
	}
	u.Values = make([][]byte, docCount)
	data := make([]byte, stringSize+5)

	copy(data[0:4], Int32Bytes(int32(stringSize)+1))

	u.recur(data, stringSize, 0, docCount)
	return nil
}

// check if current version of mongodb is greater or at least equal
// than a specific version
func (ci *CollInfo) versionAtLeast(v ...int) (result bool) {
	for i := range v {
		if i == len(ci.Version) {
			return false
		}
		if ci.Version[i] != v[i] {
			return ci.Version[i] >= v[i]
		}
	}
	return true
}

// newGenerator returns a new Generator based on a JSON configuration
func (ci *CollInfo) newGenerator(k string, v *config.GeneratorJSON) (Generator, error) {
	if v.NullPercentage > 100 {
		return nil, fmt.Errorf("for field %s, null percentage can't be > 100", k)
	}
	// use a default key of length 1. This can happen for a generator of type fromArray
	// used as generator of an ArrayGenerator
	if len(k) == 0 {
		k = "k"
	}
	// EmptyGenerator to store general info
	eg := EmptyGenerator{
		K:              append([]byte(k), byte(0)),
		NullPercentage: uint32(v.NullPercentage) * 10,
		Out:            ci.Encoder,
	}

	if v.MaxDistinctValue != 0 {
		size := v.MaxDistinctValue
		v.MaxDistinctValue = 0
		arr, t, err := ci.PreGenerate(k, v, size)
		if err != nil {
			return nil, err
		}
		eg.T = t
		return &FromArrayGenerator{
			EmptyGenerator: eg,
			Array:          arr,
			Size:           size,
			Index:          0,
		}, nil
	}

	switch v.Type {
	case "string":
		if v.MinLength < 0 || v.MinLength > v.MaxLength {
			return nil, fmt.Errorf("for field %s, make sure that MinLength >= 0 and MinLength <= MaxLength", k)
		}
		eg.T = bson.ElementString
		if v.Unique {
			// unique string can only be of fixed length, use minLength as length
			u := &UniqueGenerator{
				CurrentIndex: 0,
			}
			err := u.getUniqueArray(ci.Count, int(v.MinLength))
			if err != nil {
				return nil, fmt.Errorf("for field %s, %v", k, err)
			}
			return &FromArrayGenerator{
				EmptyGenerator: eg,
				Array:          u.Values,
				Size:           ci.Count,
				Index:          0,
			}, nil
		}
		return &StringGenerator{
			EmptyGenerator: eg,
			MinLength:      uint32(v.MinLength),
			MaxLength:      uint32(v.MaxLength),
		}, nil
	case "int":
		if v.MaxInt32 == 0 || v.MaxInt32 <= v.MinInt32 {
			return nil, fmt.Errorf("for field %s, make sure that MaxInt32 > MinInt32", k)
		}
		eg.T = bson.ElementInt32
		// Max = MaxInt32 + 1 so bound are inclusive
		return &Int32Generator{
			EmptyGenerator: eg,
			Min:            v.MinInt32,
			Max:            v.MaxInt32 + 1,
		}, nil
	case "long":
		if v.MaxInt64 == 0 || v.MaxInt64 <= v.MinInt64 {
			return nil, fmt.Errorf("for field %s, make sure that MaxInt64 > MinInt64", k)
		}
		eg.T = bson.ElementInt64
		// Max = MaxInt64 + 1 so bound are inclusive
		return &Int64Generator{
			EmptyGenerator: eg,
			Min:            v.MinInt64,
			Max:            v.MaxInt64 + 1,
		}, nil
	case "double":
		if v.MaxFloat64 == 0 || v.MaxFloat64 <= v.MinFloat64 {
			return nil, fmt.Errorf("for field %s, make sure that MaxFloat64 > MinFloat64", k)
		}
		eg.T = bson.ElementFloat64
		return &Float64Generator{
			EmptyGenerator: eg,
			Mean:           v.MinFloat64,
			StdDev:         (v.MaxFloat64 - v.MinFloat64) / 2,
		}, nil
	case "decimal":
		if !ci.versionAtLeast(3, 4) {
			return nil, fmt.Errorf("for field %s, decimal type (bson decimal128) requires mongodb 3.4 at least", k)
		}
		eg.T = bson.ElementDecimal128
		return &Decimal128Generator{
			EmptyGenerator: eg,
		}, nil
	case "boolean":
		eg.T = bson.ElementBool
		return &BoolGenerator{
			EmptyGenerator: eg,
		}, nil
	case "objectId":
		eg.T = bson.ElementObjectId
		return &ObjectIDGenerator{
			EmptyGenerator: eg,
		}, nil
	case "array":
		if v.Size <= 0 {
			return nil, fmt.Errorf("for field %s, make sure that size >= 0", k)
		}
		g, err := ci.newGenerator("", v.ArrayContent)
		if err != nil {
			return nil, fmt.Errorf("couldn't create new generator: %v", err)
		}

		// if the generator is of type FromArrayGenerator,
		// use the type of the first Element as global type
		// for the generator
		// => fromArrayGenerator currently has to contain object of
		// the same type, otherwise bson object will be incorrect
		switch g.(type) {
		case *FromArrayGenerator:
			g := g.(*FromArrayGenerator)
			// if array is generated with preGenerate(), this step is not needed
			if !g.DoNotTruncate {
				g.T = g.Array[0][0]
				// do not write first 3 bytes, ie
				// bson type, byte("k"), byte(0) to avoid conflict with
				// array index, because index is the key
				for i := range g.Array {
					g.Array[i] = g.Array[i][3:]
				}
			}
		case *ConstGenerator:
			g := g.(*ConstGenerator)
			g.T = g.Val[0]
			g.Val = g.Val[1+len(g.K):]
		default:
		}

		eg.T = bson.ElementArray
		return &ArrayGenerator{
			EmptyGenerator: eg,
			Size:           v.Size,
			Generator:      g,
		}, nil
	case "object":
		g, err := ci.newGeneratorsFromMap(v.ObjectContent)
		if err != nil {
			return nil, err
		}
		eg.T = bson.ElementDocument
		return &EmbeddedObjectGenerator{
			EmptyGenerator: eg,
			Generators:     g,
		}, nil
	case "fromArray":
		if len(v.In) == 0 {
			return nil, fmt.Errorf("for field %s, in array can't be null or empty", k)
		}
		array := make([][]byte, len(v.In))
		for i, v := range v.In {
			m := bson.M{k: v}
			raw, err := bson.Marshal(m)
			if err != nil {
				return nil, fmt.Errorf("for field %s, couldn't marshal value: %v", k, err)
			}
			array[i] = raw[4 : len(raw)-1]
		}
		eg.T = bson.ElementNil
		return &FromArrayGenerator{
			EmptyGenerator: eg,
			Array:          array,
			Size:           len(v.In),
			Index:          0,
		}, nil
	case "binary":
		if v.MinLength < 0 || v.MinLength > v.MaxLength {
			return nil, fmt.Errorf("for field %s, make sure that MinLength >= 0 and MinLength < MaxLength", k)
		}
		eg.T = bson.ElementBinary
		return &BinaryDataGenerator{
			EmptyGenerator: eg,
			MinLength:      uint32(v.MinLength),
			MaxLength:      uint32(v.MaxLength),
		}, nil
	case "date":
		if v.StartDate.Unix() > v.EndDate.Unix() {
			return nil, fmt.Errorf("for field %s, make sure StartDate < endDate", k)
		}
		eg.T = bson.ElementDatetime
		return &DateGenerator{
			EmptyGenerator: eg,
			StartDate:      uint64(v.StartDate.Unix()),
			Delta:          uint64(v.EndDate.Unix() - v.StartDate.Unix()),
		}, nil
	case "position":
		eg.T = bson.ElementArray
		return &PositionGenerator{
			EmptyGenerator: eg,
		}, nil
	case "constant":
		m := bson.M{k: v.ConstVal}
		raw, err := bson.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("for field %s, couldn't marshal value: %v", k, err)
		}
		eg.T = bson.ElementNil
		return &ConstGenerator{
			EmptyGenerator: eg,
			Val:            raw[4 : len(raw)-1],
		}, nil
	case "autoincrement":
		switch v.AutoType {
		case "int":
			eg.T = bson.ElementInt32
			return &AutoIncrementGenerator32{
				EmptyGenerator: eg,
				Counter:        v.Start32,
			}, nil
		case "long":
			eg.T = bson.ElementInt64
			return &AutoIncrementGenerator64{
				EmptyGenerator: eg,
				Counter:        v.Start64,
			}, nil
		default:
			return nil, fmt.Errorf("invalid type %v for field %v", v.Type, k)
		}
	case "faker":
		eg.T = bson.ElementString
		// use "en" lolcale for now, but should be configurable
		fk, err := faker.New("en")
		if err != nil {
			return nil, fmt.Errorf("fail to instantiate faker generator: %v", err)
		}
		var method func(f *faker.Faker) string
		switch v.Method {
		case "CellPhoneNumber":
			method = (*faker.Faker).CellPhoneNumber
		case "City":
			method = (*faker.Faker).City
		case "CityPrefix":
			method = (*faker.Faker).CityPrefix
		case "CitySuffix":
			method = (*faker.Faker).CitySuffix
		case "CompanyBs":
			method = (*faker.Faker).CompanyBs
		case "CompagnyCatchPhrase":
			method = (*faker.Faker).CompanyCatchPhrase
		case "CompanyName":
			method = (*faker.Faker).CompanyName
		case "CompanySuffix":
			method = (*faker.Faker).CompanySuffix
		case "Country":
			method = (*faker.Faker).Country
		case "DomainName":
			method = (*faker.Faker).DomainName
		case "DomainSuffix":
			method = (*faker.Faker).DomainSuffix
		case "DomainWord":
			method = (*faker.Faker).DomainWord
		case "Email":
			method = (*faker.Faker).Email
		case "FirstName":
			method = (*faker.Faker).FirstName
		case "FreeEmail":
			method = (*faker.Faker).FreeEmail
		case "JobTitle":
			method = (*faker.Faker).JobTitle
		case "LastName":
			method = (*faker.Faker).LastName
		case "Name":
			method = (*faker.Faker).Name
		case "NamePrefix":
			method = (*faker.Faker).NamePrefix
		case "NameSuffix":
			method = (*faker.Faker).NameSuffix
		case "PhoneNumber":
			method = (*faker.Faker).PhoneNumber
		case "postCode":
			method = (*faker.Faker).PostCode
		case "SafeEmail":
			method = (*faker.Faker).SafeEmail
		case "SecondaryAdress":
			method = (*faker.Faker).SecondaryAddress
		case "State":
			method = (*faker.Faker).State
		case "StateAbbr":
			method = (*faker.Faker).StateAbbr
		case "StreetAdress":
			method = (*faker.Faker).StreetAddress
		case "StreetName":
			method = (*faker.Faker).StreetName
		case "StreetSuffix":
			method = (*faker.Faker).StreetSuffix
		case "URL":
			method = (*faker.Faker).URL
		case "UserName":
			method = (*faker.Faker).UserName
		default:
			return nil, fmt.Errorf("invalid Faker method for key %v: %v", k, v.Method)
		}
		return &FakerGenerator{
			EmptyGenerator: eg,
			Faker:          fk,
			F:              method,
		}, nil
	case "ref":
		_, ok := mapRef[v.ID]
		if !ok {
			arr, t, err := ci.PreGenerate(k, v.RefContent, ci.Count)
			if err != nil {
				return nil, err
			}
			mapRef[v.ID] = arr
			mapRefType[v.ID] = t
		}
		eg.T = mapRefType[v.ID]
		return &FromArrayGenerator{
			EmptyGenerator: eg,
			Array:          mapRef[v.ID],
			Size:           len(mapRef[v.ID]),
			Index:          0,
			DoNotTruncate:  true,
		}, nil
	case "countAggregator", "valueAggregator", "boundAggregator":
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid type %v for field %v", v.Type, k)
	}
}

// newGeneratorsFromMap creates a slice of generators based on a JSON configuration map
func (ci *CollInfo) newGeneratorsFromMap(content map[string]config.GeneratorJSON) ([]Generator, error) {
	gArr := make([]Generator, 0)
	for k, v := range content {
		// if shortNames option is specified, keep only two letters for each field. This is a basic
		// optimisation to save space in mongodb and during db exchanges
		if ci.ShortNames && k != "_id" && len(k) > 2 {
			k = k[:2]
		}
		g, err := ci.newGenerator(k, &v)
		if err != nil {
			return nil, err
		}
		if g != nil {
			gArr = append(gArr, g)
		}
	}
	return gArr, nil
}

// Aggregator is a type of generator that use another collection
// to compute aggregation on it
type Aggregator struct {
	K          string
	Collection string
	Database   string
	Field      string
	Query      bson.M
	Mode       int
}

// newAggregator returns a new Aggregator based on a JSON configuration
func (ci *CollInfo) newAggregator(k string, v *config.GeneratorJSON) (*Aggregator, error) {
	if v.Query == nil || len(v.Query) == 0 {
		return nil, fmt.Errorf("for field %v, query can't be null or empty", k)
	}
	if v.Database == "" {
		return nil, fmt.Errorf("for field %v, database can't be null or empty", k)
	}
	if v.Collection == "" {
		return nil, fmt.Errorf("for field %v, collection can't be null or empty", k)
	}
	switch v.Type {
	case "countAggregator":
		return &Aggregator{
			K:          k,
			Collection: v.Collection,
			Database:   v.Database,
			Query:      v.Query,
			Mode:       CountAggregator,
		}, nil
	case "valueAggregator":
		if v.Field == "" {
			return nil, fmt.Errorf("for field %v, field can't be null or empty", k)
		}
		return &Aggregator{
			K:          k,
			Collection: v.Collection,
			Database:   v.Database,
			Field:      v.Field,
			Query:      v.Query,
			Mode:       ValueAggregator,
		}, nil
	case "boundAggregator":
		if v.Field == "" {
			return nil, fmt.Errorf("for field %v, field can't be null or empty", k)
		}
		return &Aggregator{
			K:          k,
			Collection: v.Collection,
			Database:   v.Database,
			Field:      v.Field,
			Query:      v.Query,
			Mode:       BoundAggregator,
		}, nil
	default:
		return nil, nil
	}
}

//NewAggregatorFromMap creates a slice of aggregator based on a JSON configuration map
func (ci *CollInfo) NewAggregatorFromMap(content map[string]config.GeneratorJSON) ([]Aggregator, error) {
	agArr := make([]Aggregator, 0)
	for k, v := range content {
		switch v.Type {
		case "countAggregator", "valueAggregator", "boundAggregator":
			if ci.ShortNames && k != "_id" && len(k) > 2 {
				k = k[:2]
			}
			a, err := ci.newAggregator(k, &v)
			if err != nil {
				return nil, err
			}
			agArr = append(agArr, *a)
		default:
		}
	}
	return agArr, nil
}

// CreateGenerator creates an object generator to get bson.Raw objects
func (ci *CollInfo) CreateGenerator(content map[string]config.GeneratorJSON) (*ObjectGenerator, error) {
	// create the global generator
	g, err := ci.newGeneratorsFromMap(content)
	if err != nil {
		return nil, fmt.Errorf("error while creating generators from configuration file:\n\tcause: %v", err)
	}
	return &ObjectGenerator{
		EmptyGenerator: EmptyGenerator{K: []byte(""),
			NullPercentage: 0,
			T:              bson.ElementDocument,
			Out:            ci.Encoder,
		},
		Generators: g,
	}, nil
}
