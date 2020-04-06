package generators

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/MichaelTJones/pcg"
	"github.com/manveru/faker"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// CollInfo stores global info on the collection to generate
type CollInfo struct {
	// number of document in the collection
	Count int
	// MongoDB version
	Version []int
	// seed for random generation
	Seed uint64
	// map holding references values when using a reference generator
	mapRef map[int][][]byte
	// map holding references types when using a reference generator
	mapRefType map[int]bsontype.Type
	pcg32      *pcg.PCG32
	pcg64      *pcg.PCG64
}

// NewCollInfo returns a new CollInfo.
// mapRef is a map holding bson-encoded values for references fields.
// mapRefType is a map holding bson type for references fields.
func NewCollInfo(count int, version []int, seed uint64, mapRef map[int][][]byte, mapRefType map[int]bsontype.Type) *CollInfo {
	if count <= 0 {
		count = 1
	}
	return &CollInfo{
		Count:      count,
		Version:    version,
		Seed:       seed,
		mapRef:     mapRef,
		mapRefType: mapRefType,
		pcg32:      pcg.NewPCG32().Seed(seed, seed),
		pcg64:      pcg.NewPCG64().Seed(seed, seed, seed, seed),
	}
}

// Config struct containing all possible options
type Config struct {
	// Type of object to generate, required
	Type string `json:"type"`
	// Percentage of documents that won't contains this field, optional
	NullPercentage int `json:"nullPercentage"`
	// Maximum number of distinct value for this field, optional
	MaxDistinctValue int `json:"maxDistinctValue"`
	// For `string` type only. If set to 'true', string will be unique
	Unique bool `json:"unique"`
	// For `string` and `binary` type only. Specify the Min length of the object to generate
	MinLength int `json:"minLength"`
	// For `string` and `binary` type only. Specify the Max length of the object to generate
	MaxLength int `json:"maxLength"`
	// For `int` type only. Lower bound for the int32 to generate
	MinInt int32 `json:"minInt"`
	// For `int` type only. Higher bound for the int32 to generate
	MaxInt int32 `json:"maxInt"`
	// For `long` type only. Lower bound for the int64 to generate
	MinLong int64 `json:"minLong"`
	// For `long` type only. Higher bound for the int64 to generate
	MaxLong int64 `json:"maxLong"`
	// For `double` type only. Lower bound for the float64 to generate
	MinDouble float64 `json:"minDouble"`
	// For `double` type only. Higher bound for the float64 to generate
	MaxDouble float64 `json:"maxDouble"`
	// For `array` only. Size of the array
	Size int `json:"size"`
	// For `array` only. Config to fill the array. Need to
	// pass a pointer here to avoid 'invalid recursive type' error
	ArrayContent *Config `json:"arrayContent"`
	// For `object` only. List of GeneratorJSON to generate the content
	// of the object
	ObjectContent map[string]Config `json:"objectContent"`
	// For `fromArray` only. If specified, the generator pick one of the item of the array
	In []interface{} `json:"in"`
	// For `fromArray` only. If set to true, items are picked from the array in random order
	RandomOrder bool `json:"randomOrder"`
	// For `date` only. Lower bound for the date to generate
	StartDate time.Time `json:"startDate"`
	// For `date` only. Higher bound for the date to generate
	EndDate time.Time `json:"endDate"`
	// For `constant` type only. Value of the constant field
	ConstVal interface{} `json:"constVal"`
	// For `autoincrement` type only. Start value
	StartInt int32 `json:"startInt"`
	// For `autoincrement` type only. Start value
	StartLong int64 `json:"startLong"`
	// For `autoincrement` type only. Type of the field, can be int | long
	AutoType string `json:"autoType"`
	// For `faker` type only. Method to use
	Method string `json:"method"`
	// For `ref` type only. Used to retrieve the array storing the value
	// for this field
	ID int `json:"id"`
	// For `ref` type only. generator for the field
	RefContent *Config `json:"refContent"`
	// For `countAggregator`, `boundAggregator` and `valueAggregator` only
	Collection string `json:"collection"`
	// For `countAggregator`, `boundAggregator` and `valueAggregator` only
	Database string `json:"database"`
	// For `boundAggregator` and `valueAggregator` only
	Field string `json:"field"`
	// For `countAggregator`, `boundAggregator` and `valueAggregator` only
	Query bson.M `json:"query"`
}

// available generator types, see https://github.com/feliixx/mgodatagen/blob/master/README.md#generator-types for details
const (
	TypeString        = "string"
	TypeInt           = "int"
	TypeLong          = "long"
	TypeDouble        = "double"
	TypeDecimal       = "decimal"
	TypeBoolean       = "boolean"
	TypeObjectID      = "objectId"
	TypeArray         = "array"
	TypePosition      = "position"
	TypeObject        = "object"
	TypeFromArray     = "fromArray"
	TypeConstant      = "constant"
	TypeRef           = "ref"
	TypeAutoincrement = "autoincrement"
	TypeBinary        = "binary"
	TypeDate          = "date"
	TypeUUID          = "uuid"
	TypeFaker         = "faker"
)

// available aggregator types
const (
	TypeCountAggregator = "countAggregator"
	TypeValueAggregator = "valueAggregator"
	TypeBoundAggregator = "boundAggregator"
)

// available faker methods
const (
	MethodCellPhoneNumber    = "CellPhoneNumber"
	MethodCity               = "City"
	MethodCityPrefix         = "CityPrefix"
	MethodCitySuffix         = "CitySuffix"
	MethodCompanyBs          = "CompanyBs"
	MethodCompanyCatchPhrase = "CompanyCatchPhrase"
	MethodCompanyName        = "CompanyName"
	MethodCompanySuffix      = "CompanySuffix"
	MethodCountry            = "Country"
	MethodDomainName         = "DomainName"
	MethodDomainSuffix       = "DomainSuffix"
	MethodDomainWord         = "DomainWord"
	MethodEmail              = "Email"
	MethodFirstName          = "FirstName"
	MethodFreeEmail          = "FreeEmail"
	MethodJobTitle           = "JobTitle"
	MethodLastName           = "LastName"
	MethodName               = "Name"
	MethodNamePrefix         = "NamePrefix"
	MethodNameSuffix         = "NameSuffix"
	MethodPhoneNumber        = "PhoneNumber"
	MethodPostCode           = "PostCode"
	MethodSafeEmail          = "SafeEmail"
	MethodSecondaryAddress   = "SecondaryAddress"
	MethodState              = "State"
	MethodStateAbbr          = "StateAbbr"
	MethodStreetAddress      = "StreetAddress"
	MethodStreetName         = "StreetName"
	MethodStreetSuffix       = "StreetSuffix"
	MethodURL                = "URL"
	MethodUserName           = "UserName"
)

var mapTypes = map[string]bsontype.Type{
	TypeString:        bson.TypeString,
	TypeInt:           bson.TypeInt32,
	TypeLong:          bson.TypeInt64,
	TypeDouble:        bson.TypeDouble,
	TypeDecimal:       bson.TypeDecimal128,
	TypeBoolean:       bson.TypeBoolean,
	TypeObjectID:      bson.TypeObjectID,
	TypeArray:         bson.TypeArray,
	TypePosition:      bson.TypeArray,
	TypeObject:        bson.TypeEmbeddedDocument,
	TypeFromArray:     bson.TypeNull, // can be of any bson type
	TypeConstant:      bson.TypeNull, // can be of any bson type
	TypeRef:           bson.TypeNull, // can be of any bson type
	TypeAutoincrement: bson.TypeNull, // type bson.ElementInt32 or bson.ElementInt64
	TypeBinary:        bson.TypeBinary,
	TypeDate:          bson.TypeDateTime,
	TypeUUID:          bson.TypeString,
	TypeFaker:         bson.TypeString,

	TypeCountAggregator: bson.TypeNull,
	TypeValueAggregator: bson.TypeNull,
	TypeBoundAggregator: bson.TypeNull,
}

var fakerMethods = map[string]func(f *faker.Faker) string{
	MethodCellPhoneNumber:    (*faker.Faker).CellPhoneNumber,
	MethodCity:               (*faker.Faker).City,
	MethodCityPrefix:         (*faker.Faker).CityPrefix,
	MethodCitySuffix:         (*faker.Faker).CitySuffix,
	MethodCompanyBs:          (*faker.Faker).CompanyBs,
	MethodCompanyCatchPhrase: (*faker.Faker).CompanyCatchPhrase,
	MethodCompanyName:        (*faker.Faker).CompanyName,
	MethodCompanySuffix:      (*faker.Faker).CompanySuffix,
	MethodCountry:            (*faker.Faker).Country,
	MethodDomainName:         (*faker.Faker).DomainName,
	MethodDomainSuffix:       (*faker.Faker).DomainSuffix,
	MethodDomainWord:         (*faker.Faker).DomainWord,
	MethodEmail:              (*faker.Faker).Email,
	MethodFirstName:          (*faker.Faker).FirstName,
	MethodFreeEmail:          (*faker.Faker).FreeEmail,
	MethodJobTitle:           (*faker.Faker).JobTitle,
	MethodLastName:           (*faker.Faker).LastName,
	MethodName:               (*faker.Faker).Name,
	MethodNamePrefix:         (*faker.Faker).NamePrefix,
	MethodNameSuffix:         (*faker.Faker).NameSuffix,
	MethodPhoneNumber:        (*faker.Faker).PhoneNumber,
	MethodPostCode:           (*faker.Faker).PostCode,
	MethodSafeEmail:          (*faker.Faker).SafeEmail,
	MethodSecondaryAddress:   (*faker.Faker).SecondaryAddress,
	MethodState:              (*faker.Faker).State,
	MethodStateAbbr:          (*faker.Faker).StateAbbr,
	MethodStreetAddress:      (*faker.Faker).StreetAddress,
	MethodStreetName:         (*faker.Faker).StreetName,
	MethodStreetSuffix:       (*faker.Faker).StreetSuffix,
	MethodURL:                (*faker.Faker).URL,
	MethodUserName:           (*faker.Faker).UserName,
}

// NewDocumentGenerator creates an object generator to generate valid bson documents
func (ci *CollInfo) NewDocumentGenerator(content map[string]Config) (*DocumentGenerator, error) {
	buffer := NewDocBuffer()
	d := &DocumentGenerator{
		Buffer:     buffer,
		Generators: make([]Generator, 0, len(content)),
	}
	for k, v := range content {
		g, err := ci.newGenerator(buffer, k, &v)
		if err != nil {
			return nil, fmt.Errorf("fail to create DocumentGenerator:\n\tcause: for field %s, %v", k, err)
		}
		d.Add(g)
	}
	return d, nil
}

func (ci *CollInfo) newGenerator(buffer *DocBuffer, key string, config *Config) (Generator, error) {

	if config.NullPercentage > 100 || config.NullPercentage < 0 {
		return nil, errors.New("null percentage has to be between 0 and 100")
	}
	// use a default key of length 1. This can happen for a generator of type fromArray
	// used as generator of an ArrayGenerator
	if len(key) == 0 {
		key = "k"
	}

	bsonType, ok := mapTypes[config.Type]
	if !ok {
		return nil, fmt.Errorf("invalid type %v", config.Type)
	}
	nullPercentage := uint32(config.NullPercentage) * 10
	base := newBase(key, nullPercentage, bsonType, buffer, ci.pcg32)

	if config.MaxDistinctValue != 0 {
		size := config.MaxDistinctValue
		config.MaxDistinctValue = 0
		values, bsonType, err := ci.preGenerate(key, config, size)
		if err != nil {
			return nil, err
		}
		base.bsonType = bsonType
		return &fromArrayGenerator{
			base:  base,
			array: values,
			size:  size,
			index: 0,
		}, nil
	}

	switch config.Type {
	case TypeString:
		if config.MinLength < 0 || config.MinLength > config.MaxLength {
			return nil, errors.New("make sure that 'minLength' >= 0 and 'minLength' <= 'maxLength'")
		}
		if config.Unique {
			values, err := uniqueValues(ci.Count, config.MaxLength)
			if err != nil {
				return nil, err
			}
			return &fromArrayGenerator{
				base:  base,
				array: values,
				size:  ci.Count,
				index: 0,
			}, nil
		}
		return &stringGenerator{
			base:      base,
			minLength: uint32(config.MinLength),
			maxLength: uint32(config.MaxLength),
		}, nil

	case TypeInt:
		if config.MaxInt < config.MinInt {
			return nil, errors.New("make sure that 'maxInt' >= 'minInt'")
		}
		if config.MinInt == config.MaxInt {
			return constGeneratorFromValue(base, config.MaxInt)
		}
		return &int32Generator{
			base: base,
			min:  config.MinInt,
			max:  config.MaxInt + 1,
		}, nil

	case TypeLong:
		if config.MaxLong < config.MinLong {
			return nil, errors.New("make sure that 'maxLong' >= 'minLong'")
		}
		if config.MinLong == config.MaxLong {
			return constGeneratorFromValue(base, config.MaxLong)
		}
		return &int64Generator{
			base:  base,
			min:   config.MinLong,
			max:   config.MaxLong + 1,
			pcg64: ci.pcg64,
		}, nil

	case TypeDouble:
		if config.MaxDouble < config.MinDouble {
			return nil, errors.New("make sure that 'maxDouble' >= 'minDouble'")
		}
		if config.MinDouble == config.MaxDouble {
			return constGeneratorFromValue(base, config.MaxDouble)
		}
		return &float64Generator{
			base:   base,
			mean:   config.MinDouble,
			stdDev: (config.MaxDouble - config.MinDouble) / 2,
			pcg64:  ci.pcg64,
		}, nil

	case TypeDecimal:
		if !ci.versionAtLeast(3, 4) {
			return nil, errors.New("decimal type (bson decimal128) requires mongodb 3.4 at least")
		}
		return &decimal128Generator{base: base, pcg64: ci.pcg64}, nil

	case TypeBoolean:
		return &boolGenerator{base: base}, nil

	case TypeObjectID:
		return &objectIDGenerator{base: base}, nil

	case TypeArray:
		if config.Size <= 0 {
			return nil, errors.New("make sure that 'size' >= 0")
		}
		g, err := ci.newGenerator(buffer, "", config.ArrayContent)
		if err != nil {
			return nil, err
		}

		// if the generator is of type FromArrayGenerator,
		// use the type of the first Element as global type
		// for the generator
		// => fromArrayGenerator currently has to contain object of
		// the same type, otherwise bson object will be incorrect
		switch g := g.(type) {
		case *fromArrayGenerator:
			// if array is generated with preGenerate(), this step is not needed
			if !g.doNotTruncate {
				g.bsonType = bsontype.Type(g.array[0][0])
				// do not write first 3 bytes, ie
				// bson type, byte("k"), byte(0) to avoid conflict with
				// array index, because index is the key
				for i := range g.array {
					g.array[i] = g.array[i][3:]
				}
			}
		case *constGenerator:
			g.bsonType = bsontype.Type(g.val[0])
			// 2: 1 for bson type, and 1 for terminating byte 0x00 after element key
			g.val = g.val[2+len(g.Key()):]
		default:
		}

		return &arrayGenerator{
			base:      base,
			size:      config.Size,
			generator: g,
		}, nil

	case TypeObject:
		emg := &embeddedObjectGenerator{
			base:       base,
			generators: make([]Generator, 0, len(config.ObjectContent)),
		}
		for k, v := range config.ObjectContent {
			g, err := ci.newGenerator(buffer, k, &v)
			if err != nil {
				return nil, err
			}
			if g != nil {
				emg.generators = append(emg.generators, g)
			}
		}
		return emg, nil

	case TypeFromArray:
		if len(config.In) == 0 {
			return nil, errors.New("'in' array can't be null or empty")
		}
		array := make([][]byte, len(config.In))
		for i, v := range config.In {
			raw, err := bsonValue(key, v)
			if err != nil {
				return nil, err
			}
			array[i] = raw
		}
		return &fromArrayGenerator{
			base:        base,
			array:       array,
			size:        len(config.In),
			index:       0,
			randomOrder: config.RandomOrder,
		}, nil

	case TypeBinary:
		if config.MinLength < 0 || config.MinLength > config.MaxLength {
			return nil, errors.New("make sure that 'minLength' >= 0 and 'minLength' < 'maxLength'")
		}
		return &binaryDataGenerator{
			base:      base,
			maxLength: uint32(config.MaxLength),
			minLength: uint32(config.MinLength),
		}, nil

	case TypeDate:
		if config.StartDate.Unix() > config.EndDate.Unix() {
			return nil, errors.New("make sure that 'startDate' < 'endDate'")
		}
		return &dateGenerator{
			base:      base,
			startDate: uint64(config.StartDate.Unix()),
			delta:     uint64(config.EndDate.Unix() - config.StartDate.Unix()),
			pcg64:     ci.pcg64,
		}, nil

	case TypePosition:
		return &positionGenerator{base: base, pcg64: ci.pcg64}, nil

	case TypeConstant:
		return constGeneratorFromValue(base, config.ConstVal)

	case TypeAutoincrement:
		switch config.AutoType {
		case TypeInt:
			base.bsonType = bson.TypeInt32
			return &autoIncrementGenerator32{
				base:    base,
				counter: config.StartInt,
			}, nil
		case TypeLong:
			base.bsonType = bson.TypeInt64
			return &autoIncrementGenerator64{
				base:    base,
				counter: config.StartLong,
			}, nil
		default:
			return nil, fmt.Errorf("invalid type %v", config.Type)
		}

	case TypeUUID:
		return &uuidGenerator{base: base}, nil

	case TypeFaker:
		// TODO: use "en" locale for now, but should be configurable
		fk, err := faker.New("en")
		if err != nil {
			return nil, fmt.Errorf("fail to instantiate faker generator: %v", err)
		}
		method, ok := fakerMethods[config.Method]
		if !ok {
			return nil, fmt.Errorf("invalid Faker method: %v", config.Method)
		}
		return &fakerGenerator{
			base:  base,
			faker: fk,
			f:     method,
		}, nil

	case TypeRef:
		_, ok := ci.mapRef[config.ID]
		if !ok {
			values, bsonType, err := ci.preGenerate(key, config.RefContent, ci.Count)
			if err != nil {
				return nil, err
			}
			ci.mapRef[config.ID] = values
			ci.mapRefType[config.ID] = bsonType
		}
		base.bsonType = ci.mapRefType[config.ID]
		return &fromArrayGenerator{
			base:          base,
			array:         ci.mapRef[config.ID],
			size:          len(ci.mapRef[config.ID]),
			index:         0,
			doNotTruncate: true,
		}, nil
	}
	return nil, nil
}

func constGeneratorFromValue(base base, value interface{}) (Generator, error) {
	raw, err := bsonValue(string(base.Key()), value)
	if err != nil {
		return nil, err
	}
	// the bson type is already included in raw, so make sure that it's 'unset' from base
	base.bsonType = bson.TypeNull
	return &constGenerator{
		base: base,
		val:  raw,
	}, nil
}

func bsonValue(key string, val interface{}) ([]byte, error) {
	raw, err := bson.Marshal(bson.M{key: val})
	if err != nil {
		return nil, fmt.Errorf("couldn't marshal value: %v", err)
	}
	// remove first 4 bytes (bson document size) and last bytes (terminating 0x00
	// indicating end of document) to keep only the bson content
	return raw[4 : len(raw)-1], nil
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
			return nil, fmt.Errorf("doc count is greater than possible value for string of size %v, max is %v ( %v^%v) ", stringSize, maxNumber, len(letterBytes), stringSize)
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

// preGenerate generates `nb`values using a generator created from config
func (ci *CollInfo) preGenerate(key string, config *Config, nb int) (values [][]byte, bsonType bsontype.Type, err error) {

	if nb < 0 {
		return nil, bson.TypeNull, errors.New("maxDistinctValue can't be negative")
	}

	buffer := NewDocBuffer()
	tmpCi := NewCollInfo(ci.Count, ci.Version, ci.Seed, ci.mapRef, ci.mapRefType)
	g, err := tmpCi.newGenerator(buffer, key, config)
	if err != nil {
		return nil, bson.TypeNull, fmt.Errorf("error while creating base array: %v", err)
	}

	values = make([][]byte, nb)
	for i := 0; i < nb; i++ {
		g.Value()
		tmpArr := make([]byte, buffer.Len())
		copy(tmpArr, buffer.Bytes())
		values[i] = tmpArr
		buffer.Truncate(0)
	}
	if nb > 1 {
		if bytes.Equal(values[0], values[1]) {
			return nil, bson.TypeNull, errors.New("couldn't generate enough unique values")
		}
	}
	return values, g.Type(), nil
}

// NewAggregatorSlice creates a slice of Aggregator from a map of Config
func (ci *CollInfo) NewAggregatorSlice(content map[string]Config) ([]Aggregator, error) {
	return ci.newAggregatorFromMap(content)
}

func (ci *CollInfo) newAggregatorFromMap(content map[string]Config) ([]Aggregator, error) {
	agArr := make([]Aggregator, 0)
	for k, v := range content {
		switch v.Type {
		case TypeCountAggregator, TypeValueAggregator, TypeBoundAggregator:
			a, err := ci.newAggregator(k, &v)
			if err != nil {
				return nil, fmt.Errorf("for field %s, %v", k, err)
			}
			agArr = append(agArr, a)
		default:
		}
	}
	return agArr, nil
}

func (ci *CollInfo) newAggregator(key string, config *Config) (Aggregator, error) {

	if config.Query == nil || len(config.Query) == 0 {
		return nil, errors.New("'query' can't be null or empty")
	}
	if config.Database == "" {
		return nil, errors.New("'database' can't be null or empty")
	}
	if config.Collection == "" {
		return nil, errors.New("'collection' can't be null or empty")
	}

	localVar := "_id"
	for _, v := range config.Query {
		vStr := fmt.Sprintf("%v", v)
		if len(vStr) >= 2 && vStr[:2] == "$$" {
			localVar = vStr[2:]
		}
	}

	base := baseAggregator{
		key:        key,
		query:      config.Query,
		collection: config.Collection,
		database:   config.Database,
		localVar:   localVar,
	}
	switch config.Type {
	case TypeCountAggregator:
		return &countAggregator{baseAggregator: base}, nil

	case TypeValueAggregator:
		if config.Field == "" {
			return nil, errors.New("'field' can't be null or empty")
		}
		return &valueAggregator{baseAggregator: base, field: config.Field}, nil

	case TypeBoundAggregator:
		if config.Field == "" {
			return nil, errors.New("'field' can't be null or empty")
		}
		return &boundAggregator{baseAggregator: base, field: config.Field}, nil
	}
	return nil, nil
}
