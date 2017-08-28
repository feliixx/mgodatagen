// Package generators used to create object implementing Generator interface.
// Each Generator generate a random value of a specific BSON type.
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
//
// It was created as part of mgodatagen, but is standalone
// and may be used on its own.
package generators

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/globalsign/mgo/bson"
)

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
	letterIdxBits = 6                    // 6 bits to represent a letter index (2^6 => 0-63)
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var (
	// main available types
	mapType = map[string]int{
		"string":        0, // string
		"boolean":       1, // bool
		"int":           2, // int32
		"long":          3, // int64
		"double":        4, // float64
		"objectId":      5, // bson.ObjectId
		"object":        6, // bson.M
		"date":          7, // time.Time
		"autoincrement": 3, // int64
		"other":         8,
	}
	// stores value for ref fields
	mapRef = map[int][]interface{}{}
)

// GeneratorJSON struct containing all possible options
type GeneratorJSON struct {
	// Type of object to genereate.
	Type string `json:"type"`
	// For `string` and `binary` type only. Specify the Min length of the object to generate
	MinLength int32 `json:"MinLength"`
	// For `string` and `binary` type only. Specify the Max length of the object to generate
	MaxLength int32 `json:"MaxLength"`
	// Percentage of documents that won't contains this field
	NullPercentage int64 `json:"nullPercentage"`
	// For `int` type only. Lower bound for the int32 to generate
	MinInt32 int32 `json:"MinInt"`
	// For `int` type only. Higher bound for the int32 to generate
	MaxInt32 int32 `json:"MaxInt"`
	// For `long` type only. Lower bound for the int64 to generate
	MinInt64 int64 `json:"MinLong"`
	// For `long` type only. Higher bound for the int64 to generate
	MaxInt64 int64 `json:"MaxLong"`
	// For `double` type only. Lower bound for the float64 to generate
	MinFloat64 float64 `json:"MinDouble"`
	// For `double` type only. Higher bound for the float64 to generate
	MaxFloat64 float64 `json:"MaxDouble"`
	// For `array` only. Size of the array
	Size int `json:"size"`
	// For `array` only. GeneratorJSON to fill the array. Need to
	// pass a pointer here to avoid 'invalid recursive type' error
	ArrayContent *GeneratorJSON `json:"arrayContent"`
	// For `object` only. List of GeneratorJSON to generate the content
	// of the object
	ObjectContent map[string]GeneratorJSON `json:"objectContent"`
	// For `fromArray` only. If specified, the generator pick one of the item of the array
	In []interface{} `json:"in"`
	// For `date` only. Lower bound for the date to generate
	StartDate time.Time `json:"StartDate"`
	// For `date` only. Higher bound for the date to generate
	EndDate time.Time `json:"endDate"`
	// For `constant` type only. Value of the constant field
	ConstVal interface{} `json:"constVal"`
	// For `autoincrement` type only. Start value
	Counter int64 `json:"counter"`
	// For `ref` type only. Used to retrieve the array storing the value
	// for this field
	ID int `json:"id"`
	// For `ref` type only. generator for the field
	RefContent *GeneratorJSON `json:"refContent"`
}

// RandSource stores ressources to get random value. Keep both as
// src.int63() is faster than r.int63().
type RandSource struct {
	Src rand.Source
	R   *rand.Rand
}

// NewRandSource creates a new RandSource
func NewRandSource() *RandSource {
	var rndSrc = rand.NewSource(time.Now().UnixNano())
	return &RandSource{R: rand.New(rndSrc), Src: rndSrc}
}

// Generator interface for all generator objects
type Generator interface {
	Key() string
	Type() int
	// Get a random value according to the generator type
	Value(rnd *RandSource) interface{}
	Exists(rnd *RandSource) bool
}

// EmptyGenerator serves as base for the actual generators
type EmptyGenerator struct {
	K              string
	NullPercentage int64
	T              int
}

// Key returns the key of the object
func (g *EmptyGenerator) Key() string { return g.K }

// Exists returns true if the generation should be performed
// get the last 10 bits of the random int64 to get a number between 0 and 1023,
// and compare it to nullPercentage * 10
func (g *EmptyGenerator) Exists(rnd *RandSource) bool {
	if g.NullPercentage == 0 {
		return true
	}
	return rnd.Src.Int63()>>53 >= g.NullPercentage
}

// Type returns an int corresponding to a type from mapType
func (g *EmptyGenerator) Type() int { return g.T }

// StringGenerator struct that implements Generator. Used to
// generate random string of `length` length
type StringGenerator struct {
	EmptyGenerator
	MinLength int32
	MaxLength int32
}

// Value returns a random String of `g.length` length
func (g *StringGenerator) Value(rnd *RandSource) interface{} {
	length := g.MinLength
	if g.MaxLength != g.MinLength {
		length = rnd.R.Int31n(g.MaxLength-g.MinLength) + g.MinLength
	}
	b := make([]byte, length)
	cache, remain := rnd.Src.Int63(), letterIdxMax
	for i := length - 1; i >= 0; i-- {
		if remain == 0 {
			cache, remain = rnd.Src.Int63(), letterIdxMax
		}
		b[i] = letterBytes[cache&letterIdxMask]
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

// Int32Generator struct that implements Generator. Used to
// generate random int32 between `Min` and `Max`
type Int32Generator struct {
	EmptyGenerator
	Min int32
	Max int32
}

// Value returns a random int32 between `g.Min` and `g.Max`
func (g *Int32Generator) Value(rnd *RandSource) interface{} { return rnd.R.Int31n(g.Max-g.Min) + g.Min }

// Int64Generator struct that implements Generator. Used to
// generate random int64 between `Min` and `Max`
type Int64Generator struct {
	EmptyGenerator
	Min int64
	Max int64
}

// Value returns a random int64 between `g.Min` and `g.Max`
func (g *Int64Generator) Value(rnd *RandSource) interface{} { return rnd.R.Int63n(g.Max-g.Min) + g.Min }

// Float64Generator struct that implements Generator. Used to
// generate random int64 between `Min` and `Max`
type Float64Generator struct {
	EmptyGenerator
	Mean   float64
	StdDev float64
}

// Value returns a random float64 between `g.Min` and `g.Max`
func (g *Float64Generator) Value(rnd *RandSource) interface{} {
	return rnd.R.Float64()*g.StdDev + g.Mean
}

// BoolGenerator struct that implements Generator. Used to
// generate random bool
type BoolGenerator struct {
	EmptyGenerator
}

// Value returns a random boolean. (check if first bit of a random int64 is 0 )
func (g *BoolGenerator) Value(rnd *RandSource) interface{} { return rnd.Src.Int63()&0x01 == 0 }

// ArrayGenerator struct that implements Generator. Used to
// generate random array
type ArrayGenerator struct {
	EmptyGenerator
	Size      int
	Generator Generator
}

// Value returns a random array of `g.size` size. It's feed with the
// provided generator
func (g *ArrayGenerator) Value(rnd *RandSource) interface{} {
	switch g.Generator.Type() {
	case 0:
		array := make([]string, g.Size)
		for i := range array {
			array[i] = g.Generator.Value(rnd).(string)
		}
		return array
	case 1:
		array := make([]bool, g.Size)
		for i := range array {
			array[i] = g.Generator.Value(rnd).(bool)
		}
		return array
	case 2:
		array := make([]int64, g.Size)
		for i := range array {
			array[i] = g.Generator.Value(rnd).(int64)
		}
		return array
	case 3:
		array := make([]int32, g.Size)
		for i := range array {
			array[i] = g.Generator.Value(rnd).(int32)
		}
		return array
	case 4:
		array := make([]float64, g.Size)
		for i := range array {
			array[i] = g.Generator.Value(rnd).(float64)
		}
		return array
	case 5:
		array := make([]bson.ObjectId, g.Size)
		for i := range array {
			array[i] = g.Generator.Value(rnd).(bson.ObjectId)
		}
		return array
	case 6:
		array := make([]bson.M, g.Size)
		for i := range array {
			array[i] = g.Generator.Value(rnd).(bson.M)
		}
		return array
	case 7:
		array := make([]time.Time, g.Size)
		for i := range array {
			array[i] = g.Generator.Value(rnd).(time.Time)
		}
		return array
	default:
		array := make([]interface{}, g.Size)
		for i := range array {
			array[i] = g.Generator.Value(rnd)
		}
		return array
	}
}

// ObjectGenerator struct that implements Generator. Used to
// generate random object
type ObjectGenerator struct {
	EmptyGenerator
	Generators []Generator
}

// Value returns a random object
func (g *ObjectGenerator) Value(rnd *RandSource) interface{} {
	m := bson.M{}
	for _, gen := range g.Generators {
		if gen.Exists(rnd) {
			m[gen.Key()] = gen.Value(rnd)
		}
	}
	return m
}

// FromArrayGenerator struct that implements Generator. Used to
// generate a random value from an array of user-defined values
type FromArrayGenerator struct {
	EmptyGenerator
	Size  int32
	Array []interface{}
	Index int32
}

// Value returns a random item of the input array
func (g *FromArrayGenerator) Value(rnd *RandSource) interface{} {
	g.Index++
	if g.Index == g.Size {
		g.Index = 0
	}
	return g.Array[g.Index]
}

// ObjectIDGenerator struct that implements Generator. Used to
// generate bson.ObjectId
type ObjectIDGenerator struct {
	EmptyGenerator
}

// Value returns a bson.ObjectId
func (g *ObjectIDGenerator) Value(rnd *RandSource) interface{} {
	return bson.NewObjectId()
}

// BinaryDataGenerator struct that implements Generator. Used to
// generate random binary data
type BinaryDataGenerator struct {
	EmptyGenerator
	MinLength int32
	MaxLength int32
}

// Value returns a random array of bytes of length `g.length`
func (g *BinaryDataGenerator) Value(rnd *RandSource) interface{} {
	length := g.MinLength
	if g.MaxLength != g.MinLength {
		length = rnd.R.Int31n(g.MaxLength-g.MinLength) + g.MinLength
	}
	b := make([]byte, length)
	rnd.R.Read(b) // returns len(b) and a nil error
	return b
}

// DateGenerator struct that implements Generator. Used to
// generate random date within bounds
type DateGenerator struct {
	EmptyGenerator
	StartDate int64
	Delta     int64
}

// Value returns a random date within `g.StartDate` and `g.endDate`
// Date are not evenly distributed
func (g *DateGenerator) Value(rnd *RandSource) interface{} {
	return time.Unix(rnd.R.Int63n(g.Delta)+g.StartDate, 0)
}

// PositionGenerator struct that implements Generator. Used to
// generate random GPS coordinates
type PositionGenerator struct {
	EmptyGenerator
}

// Value returns a random position.
func (g *PositionGenerator) Value(rnd *RandSource) interface{} {
	return [2]float32{rnd.R.Float32()*180 - 90, rnd.R.Float32()*360 - 180}
}

// ConstGenerator struct that implements Generator. Used to
// generate constant value
type ConstGenerator struct {
	EmptyGenerator
	Val interface{}
}

// Value always returns the same value as specified in the config file
func (g *ConstGenerator) Value(rnd *RandSource) interface{} { return g.Val }

// AutoIncrementGenerator struct that implements Generator. Used to
// generate auto-incremented int64
type AutoIncrementGenerator struct {
	EmptyGenerator
	Counter int64
}

// Value returns prev counter +1, starting from `g.counter`
func (g *AutoIncrementGenerator) Value(rnd *RandSource) interface{} {
	g.Counter++
	return g.Counter
}

// RefGenerator struct that implements Generator. Generate random
// objects and store them in mapRef
type RefGenerator struct {
	EmptyGenerator
	ID        int
	Generator Generator
}

// Value returns create a random value and stores it in a slice
// hold in a map along with its id, so it can be used in fromArray
// generator
func (g *RefGenerator) Value(rnd *RandSource) interface{} {
	v := g.Generator.Value(rnd)
	mapRef[g.ID] = append(mapRef[g.ID], v)
	return v
}

// NewGenerator returns a new Generator based on a JSON configuration
func NewGenerator(k string, v *GeneratorJSON, shortNames bool) (Generator, error) {
	intType, ok := mapType[v.Type]
	if !ok {
		intType = 8
	}
	// if shortNames option is specified, keep only two letters for each field. This is a basic
	// optimisation to save space in mongodb and during db exchanges
	if shortNames && k != "_id" && len(k) > 2 {
		k = k[:2]
	}
	eg := EmptyGenerator{K: k, NullPercentage: v.NullPercentage * 10, T: intType}
	switch v.Type {
	case "string":
		if v.MinLength < 0 || v.MinLength > v.MaxLength {
			return nil, fmt.Errorf("for object %s, make sure that MinLength >= 0 and MinLength < MaxLength", k)
		}
		return &StringGenerator{EmptyGenerator: eg, MinLength: v.MinLength, MaxLength: v.MaxLength}, nil
	case "int":
		if v.MaxInt32 == 0 || v.MaxInt32 <= v.MinInt32 {
			return nil, fmt.Errorf("for object %s, make sure that MaxInt32 > MinInt32", k)
		}
		return &Int32Generator{EmptyGenerator: eg, Min: v.MinInt32, Max: v.MaxInt32}, nil
	case "long":
		if v.MaxInt64 == 0 || v.MaxInt64 <= v.MinInt64 {
			return nil, fmt.Errorf("for object %s, make sure that MaxInt64 > MinInt64", k)
		}
		return &Int64Generator{EmptyGenerator: eg, Min: v.MinInt64, Max: v.MaxInt64}, nil
	case "double":
		if v.MaxFloat64 == 0 || v.MaxFloat64 <= v.MinFloat64 {
			return nil, fmt.Errorf("for object %s, make sure that MaxFloat64 > MinFloat64", k)
		}
		return &Float64Generator{EmptyGenerator: eg, Mean: v.MinFloat64, StdDev: (v.MaxFloat64 - v.MinFloat64) / 2}, nil
	case "boolean":
		return &BoolGenerator{EmptyGenerator: eg}, nil
	case "objectId":
		return &ObjectIDGenerator{EmptyGenerator: eg}, nil
	case "array":
		if v.Size < 0 {
			return nil, fmt.Errorf("for object %s, make sure that size >= 0", k)
		}
		g, err := NewGenerator("", v.ArrayContent, shortNames)
		if err != nil {
			return nil, fmt.Errorf("for object %s, make sure that size >= 0", k)
		}
		return &ArrayGenerator{EmptyGenerator: eg, Size: v.Size, Generator: g}, nil
	case "object":
		g, err := NewGeneratorsFromMap(v.ObjectContent, shortNames)
		if err != nil {
			return nil, err
		}
		return &ObjectGenerator{EmptyGenerator: eg, Generators: g}, nil
	case "fromArray":
		if len(v.In) == 0 {
			return nil, fmt.Errorf("for object %s, in array can't be null or empty", k)
		}
		return &FromArrayGenerator{EmptyGenerator: eg, Array: v.In, Size: int32(len(v.In)), Index: -1}, nil
	case "binary":
		if v.MinLength < 0 || v.MinLength > v.MaxLength {
			return nil, fmt.Errorf("for object %s, make sure that MinLength >= 0 and MinLength < MaxLength", k)
		}
		return &BinaryDataGenerator{EmptyGenerator: eg, MinLength: v.MinLength, MaxLength: v.MaxLength}, nil
	case "date":
		if v.StartDate.Unix() > v.EndDate.Unix() {
			return nil, fmt.Errorf("for object %s, make sure StartDate < endDate", k)
		}
		return &DateGenerator{EmptyGenerator: eg, StartDate: v.StartDate.Unix(), Delta: (v.EndDate.Unix() - v.StartDate.Unix())}, nil
	case "position":
		return &PositionGenerator{EmptyGenerator: eg}, nil
	case "constant":
		return &ConstGenerator{EmptyGenerator: eg, Val: v.ConstVal}, nil
	case "autoincrement":
		return &AutoIncrementGenerator{EmptyGenerator: eg, Counter: v.Counter}, nil
	case "ref":
		_, ok := mapRef[v.ID]
		if !ok {
			g, err := NewGenerator("", v.RefContent, shortNames)
			if err != nil {
				return nil, fmt.Errorf("for object %s, %s", k, err.Error())
			}
			var arr []interface{}
			mapRef[v.ID] = arr
			return &RefGenerator{EmptyGenerator: eg, ID: v.ID, Generator: g}, nil
		}
		return &FromArrayGenerator{EmptyGenerator: eg, Array: mapRef[v.ID], Size: int32(len(mapRef[v.ID])), Index: -1}, nil
	default:
		return &BoolGenerator{EmptyGenerator: eg}, nil
	}
}

// NewGeneratorsFromMap creates a slice of generators based on a JSON configuration map
func NewGeneratorsFromMap(content map[string]GeneratorJSON, shortNames bool) ([]Generator, error) {
	gArr := make([]Generator, 0)
	for k, v := range content {
		g, err := NewGenerator(k, &v, shortNames)
		if err != nil {
			return nil, err
		}
		gArr = append(gArr, g)
	}
	return gArr, nil
}
