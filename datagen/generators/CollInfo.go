package generators

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/MichaelTJones/pcg"
	"github.com/globalsign/mgo/bson"
	"github.com/manveru/faker"
)

// CollInfo stores global info on the collection to generate
type CollInfo struct {
	// number of document in the collection
	Count int
	// if set to true, keep only first two chars for each key
	ShortNames bool
	// MongoDB version
	Version []int
	// seed for random generation
	Seed uint64
	// buffer to hold bson documents bytes
	Encoder *Encoder
	pcg32   *pcg.PCG32
	pcg64   *pcg.PCG64
}

// NewCollInfo returns a new CollInfo. If count is <=0, it will be set to 1
func NewCollInfo(count int, version []int, seed uint64) *CollInfo {
	if count <= 0 {
		count = 1
	}
	return &CollInfo{
		Count:   count,
		Version: version,
		Seed:    seed,
		Encoder: NewEncoder(),
		pcg32:   pcg.NewPCG32().Seed(seed, seed),
		pcg64:   pcg.NewPCG64().Seed(seed, seed, seed, seed),
	}
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

// Config struct containing all possible options
type Config struct {
	// Type of object to generate, required
	// available types are:
	//  - string
	//  - int
	//  - long
	//  - double
	//  - decimal
	//  - boolean
	//  - date
	//  - objectId
	//  - object
	//  - array
	//  - fromArray
	//  - binary data
	//  - position
	//  - ref
	//  - autoincrement
	//  - faker
	//  - countAggregator
	//  - valueAggregator
	//  - boundAggregator
	//
	// see https://github.com/feliixx/mgodatagen/blob/master/README.md#generator-types for details
	Type string `json:"type"`
	// Percentage of documents that won't contains this field, optional
	NullPercentage int `json:"nullPercentage"`
	// Maximum number of distinct value for this field, optional
	MaxDistinctValue int `json:"maxDistinctValue"`
	// For `string` type only. If set to 'true', string will be unique
	Unique bool `json:"unique"`
	// For `string` and `binary` type only. Specify the Min length of the object to generate
	MinLength int `json:"MinLength"`
	// For `string` and `binary` type only. Specify the Max length of the object to generate
	MaxLength int `json:"MaxLength"`
	// For `int` type only. Lower bound for the int32 to generate
	MinInt int32 `json:"MinInt"`
	// For `int` type only. Higher bound for the int32 to generate
	MaxInt int32 `json:"MaxInt"`
	// For `long` type only. Lower bound for the int64 to generate
	MinLong int64 `json:"MinLong"`
	// For `long` type only. Higher bound for the int64 to generate
	MaxLong int64 `json:"MaxLong"`
	// For `double` type only. Lower bound for the float64 to generate
	MinDouble float64 `json:"MinDouble"`
	// For `double` type only. Higher bound for the float64 to generate
	MaxDouble float64 `json:"MaxDouble"`
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
	// For `date` only. Lower bound for the date to generate
	StartDate time.Time `json:"StartDate"`
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

var bsonTypeMap = map[string]byte{
	"string":        bson.ElementString,
	"faker":         bson.ElementString,
	"int":           bson.ElementInt32,
	"long":          bson.ElementInt64,
	"double":        bson.ElementFloat64,
	"decimal":       bson.ElementDecimal128,
	"boolean":       bson.ElementBool,
	"objectId":      bson.ElementObjectId,
	"array":         bson.ElementArray,
	"position":      bson.ElementArray,
	"object":        bson.ElementDocument,
	"fromArray":     bson.ElementNil,
	"constant":      bson.ElementNil,
	"ref":           bson.ElementNil,
	"autoincrement": bson.ElementNil,
	"binary":        bson.ElementBinary,
	"date":          bson.ElementDatetime,

	"countAggregator": bson.ElementNil,
	"valueAggregator": bson.ElementNil,
	"boundAggregator": bson.ElementNil,
}

var fakerMethods = map[string]func(f *faker.Faker) string{
	"CellPhoneNumber":    (*faker.Faker).CellPhoneNumber,
	"City":               (*faker.Faker).City,
	"CityPrefix":         (*faker.Faker).CityPrefix,
	"CitySuffix":         (*faker.Faker).CitySuffix,
	"CompanyBs":          (*faker.Faker).CompanyBs,
	"CompanyCatchPhrase": (*faker.Faker).CompanyCatchPhrase,
	"CompanyName":        (*faker.Faker).CompanyName,
	"CompanySuffix":      (*faker.Faker).CompanySuffix,
	"Country":            (*faker.Faker).Country,
	"DomainName":         (*faker.Faker).DomainName,
	"DomainSuffix":       (*faker.Faker).DomainSuffix,
	"DomainWord":         (*faker.Faker).DomainWord,
	"Email":              (*faker.Faker).Email,
	"FirstName":          (*faker.Faker).FirstName,
	"FreeEmail":          (*faker.Faker).FreeEmail,
	"JobTitle":           (*faker.Faker).JobTitle,
	"LastName":           (*faker.Faker).LastName,
	"Name":               (*faker.Faker).Name,
	"NamePrefix":         (*faker.Faker).NamePrefix,
	"NameSuffix":         (*faker.Faker).NameSuffix,
	"PhoneNumber":        (*faker.Faker).PhoneNumber,
	"PostCode":           (*faker.Faker).PostCode,
	"SafeEmail":          (*faker.Faker).SafeEmail,
	"SecondaryAddress":   (*faker.Faker).SecondaryAddress,
	"State":              (*faker.Faker).State,
	"StateAbbr":          (*faker.Faker).StateAbbr,
	"StreetAddress":      (*faker.Faker).StreetAddress,
	"StreetName":         (*faker.Faker).StreetName,
	"StreetSuffix":       (*faker.Faker).StreetSuffix,
	"URL":                (*faker.Faker).URL,
	"UserName":           (*faker.Faker).UserName,
}

// NewGenerator returns a new Generator based on config
func (ci *CollInfo) NewGenerator(key string, config *Config) (Generator, error) {
	if config.NullPercentage > 100 || config.NullPercentage < 0 {
		return nil, fmt.Errorf("for field %s, null percentage has to be between 0 and 100", key)
	}
	// use a default key of length 1. This can happen for a generator of type fromArray
	// used as generator of an ArrayGenerator
	if len(key) == 0 {
		key = "k"
	}

	bsonType, ok := bsonTypeMap[config.Type]
	if !ok {
		return nil, fmt.Errorf("invalid type %v for field %v", config.Type, key)
	}
	nullPercentage := uint32(config.NullPercentage) * 10
	base := newBase(key, nullPercentage, bsonType, ci.Encoder, ci.pcg32)

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
	case "string":
		if config.MinLength < 0 || config.MinLength > config.MaxLength {
			return nil, fmt.Errorf("for field %s, make sure that 'minLength' >= 0 and 'minLength' <= 'maxLength'", key)
		}
		if config.Unique {
			values, err := uniqueValues(ci.Count, int(config.MaxLength))
			if err != nil {
				return nil, fmt.Errorf("for field %s, %v", key, err)
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
	case "int":
		if config.MaxInt == 0 || config.MaxInt <= config.MinInt {
			return nil, fmt.Errorf("for field %s, make sure that 'maxInt' > 'minInt'", key)
		}
		return &int32Generator{
			base: base,
			min:  config.MinInt,
			max:  config.MaxInt + 1,
		}, nil
	case "long":
		if config.MaxLong == 0 || config.MaxLong <= config.MinLong {
			return nil, fmt.Errorf("for field %s, make sure that 'maxLong' > 'minLong'", key)
		}
		return &int64Generator{
			base:  base,
			min:   config.MinLong,
			max:   config.MaxLong + 1,
			pcg64: ci.pcg64,
		}, nil
	case "double":
		if config.MaxDouble == 0 || config.MaxDouble <= config.MinDouble {
			return nil, fmt.Errorf("for field %s, make sure that 'maxDouble' > 'minDouble'", key)
		}
		return &float64Generator{
			base:   base,
			mean:   config.MinDouble,
			stdDev: (config.MaxDouble - config.MinDouble) / 2,
			pcg64:  ci.pcg64,
		}, nil
	case "decimal":
		if !ci.versionAtLeast(3, 4) {
			return nil, fmt.Errorf("for field %s, decimal type (bson decimal128) requires mongodb 3.4 at least", key)
		}
		return &decimal128Generator{base: base, pcg64: ci.pcg64}, nil
	case "boolean":
		return &boolGenerator{base: base}, nil
	case "objectId":
		return &objectIDGenerator{base: base}, nil
	case "array":
		if config.Size <= 0 {
			return nil, fmt.Errorf("for field %s, make sure that 'size' >= 0", key)
		}
		g, err := ci.NewGenerator("", config.ArrayContent)
		if err != nil {
			return nil, fmt.Errorf("couldn't create new generator: %v", err)
		}

		// if the generator is of type FromArrayGenerator,
		// use the type of the first Element as global type
		// for the generator
		// => fromArrayGenerator currently has to contain object of
		// the same type, otherwise bson object will be incorrect
		switch g.(type) {
		case *fromArrayGenerator:
			g := g.(*fromArrayGenerator)
			// if array is generated with preGenerate(), this step is not needed
			if !g.doNotTruncate {
				g.bsonType = g.array[0][0]
				// do not write first 3 bytes, ie
				// bson type, byte("k"), byte(0) to avoid conflict with
				// array index, because index is the key
				for i := range g.array {
					g.array[i] = g.array[i][3:]
				}
			}
		case *constGenerator:
			g := g.(*constGenerator)
			g.bsonType = g.val[0]
			g.val = g.val[1+len(g.Key()):]
		default:
		}

		return &arrayGenerator{
			base:      base,
			size:      config.Size,
			generator: g,
		}, nil
	case "object":
		emg := &embeddedObjectGenerator{
			base:       base,
			generators: make([]Generator, 0, len(config.ObjectContent)),
		}
		for k, v := range config.ObjectContent {
			g, err := ci.NewGenerator(k, &v)
			if err != nil {
				return nil, fmt.Errorf("for field %s: %v", key, err)
			}
			if g != nil {
				emg.generators = append(emg.generators, g)
			}
		}
		return emg, nil

	case "fromArray":
		if len(config.In) == 0 {
			return nil, fmt.Errorf("for field %s, 'in' array can't be null or empty", key)
		}
		array := make([][]byte, len(config.In))
		for i, v := range config.In {
			m := bson.M{key: v}
			raw, err := bson.Marshal(m)
			if err != nil {
				return nil, fmt.Errorf("for field %s, couldn't marshal value: %v", key, err)
			}
			// remove first 4 bytes (bson document size) adn last bytes (terminating 0x00
			// indicating end of document) to keep only the bson content
			array[i] = raw[4 : len(raw)-1]
		}
		return &fromArrayGenerator{
			base:  base,
			array: array,
			size:  len(config.In),
			index: 0,
		}, nil
	case "binary":
		if config.MinLength < 0 || config.MinLength > config.MaxLength {
			return nil, fmt.Errorf("for field %s, make sure that 'minLength' >= 0 and 'minLength' < 'maxLength'", key)
		}
		return &binaryDataGenerator{
			base:      base,
			maxLength: uint32(config.MaxLength),
			minLength: uint32(config.MinLength),
		}, nil
	case "date":
		if config.StartDate.Unix() > config.EndDate.Unix() {
			return nil, fmt.Errorf("for field %s, make sure that 'startDate' < 'endDate'", key)
		}
		return &dateGenerator{
			base:      base,
			startDate: uint64(config.StartDate.Unix()),
			delta:     uint64(config.EndDate.Unix() - config.StartDate.Unix()),
			pcg64:     ci.pcg64,
		}, nil
	case "position":
		return &positionGenerator{base: base, pcg64: ci.pcg64}, nil
	case "constant":
		m := bson.M{key: config.ConstVal}
		raw, err := bson.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("for field %s, couldn't marshal value: %v", key, err)
		}
		return &constGenerator{
			base: base,
			// remove first 4 bytes (bson document size) adn last bytes (terminating 0x00
			// indicating end of document) to keep only the bson content
			val: raw[4 : len(raw)-1],
		}, nil
	case "autoincrement":
		switch config.AutoType {
		case "int":
			base.bsonType = bson.ElementInt32
			return &autoIncrementGenerator32{
				base:    base,
				counter: config.StartInt,
			}, nil
		case "long":
			base.bsonType = bson.ElementInt64
			return &autoIncrementGenerator64{
				base:    base,
				counter: config.StartLong,
			}, nil
		default:
			return nil, fmt.Errorf("invalid type %v for field %v", config.Type, key)
		}
	case "faker":
		// TODO: use "en" locale for now, but should be configurable
		fk, err := faker.New("en")
		if err != nil {
			return nil, fmt.Errorf("fail to instantiate faker generator: %v", err)
		}
		method, ok := fakerMethods[config.Method]
		if !ok {
			return nil, fmt.Errorf("invalid Faker method for key %v: %v", key, config.Method)
		}
		return &fakerGenerator{
			base:  base,
			faker: fk,
			f:     method,
		}, nil
	case "ref":
		_, ok := mapRef[config.ID]
		if !ok {
			arr, t, err := ci.preGenerate(key, config.RefContent, ci.Count)
			if err != nil {
				return nil, err
			}
			mapRef[config.ID] = arr
			mapRefType[config.ID] = t
		}
		base.bsonType = mapRefType[config.ID]
		return &fromArrayGenerator{
			base:          base,
			array:         mapRef[config.ID],
			size:          len(mapRef[config.ID]),
			index:         0,
			doNotTruncate: true,
		}, nil
	}
	return nil, nil
}

// DocumentGenerator creates an object generator to generate valid bson documents
func (ci *CollInfo) DocumentGenerator(content map[string]Config) (*DocumentGenerator, error) {
	d := &DocumentGenerator{
		base:       newBase("", 0, bson.ElementDocument, ci.Encoder, ci.pcg32),
		generators: make([]Generator, 0, len(content)),
	}
	for k, v := range content {
		g, err := ci.NewGenerator(k, &v)
		if err != nil {
			return nil, fmt.Errorf("fail to create DocumentGenerator:\n\tcause: %v", err)
		}
		d.Add(g)
	}
	return d, nil
}

// preGenerate generate `nb`values using a generator created from config
func (ci *CollInfo) preGenerate(key string, config *Config, nb int) (values [][]byte, bsonType byte, err error) {

	tmpCi := NewCollInfo(ci.Count, ci.Version, ci.Seed)
	g, err := tmpCi.NewGenerator(key, config)
	if err != nil {
		return nil, bson.ElementNil, fmt.Errorf("for field %s, error while creating base array: %v", key, err)
	}

	values = make([][]byte, nb)
	for i := 0; i < nb; i++ {
		g.Value()
		tmpArr := make([]byte, tmpCi.Encoder.Len())
		copy(tmpArr, tmpCi.Encoder.Bytes())
		values[i] = tmpArr
		tmpCi.Encoder.Truncate(0)
	}
	if nb > 1 {
		if bytes.Equal(values[0], values[1]) {
			return nil, bson.ElementNil, fmt.Errorf("for field %s, couldn't generate enough unique values", key)
		}
	}
	return values, g.Type(), nil
}

// NewAggregator returns a new Aggregator based on config
func (ci *CollInfo) NewAggregator(key string, config *Config) (Aggregator, error) {
	if config.Query == nil || len(config.Query) == 0 {
		return nil, fmt.Errorf("for field %v, 'query' can't be null or empty", key)
	}
	if config.Database == "" {
		return nil, fmt.Errorf("for field %v, 'database' can't be null or empty", key)
	}
	if config.Collection == "" {
		return nil, fmt.Errorf("for field %v, 'collection' can't be null or empty", key)
	}

	localVar := "_id"
	for _, v := range config.Query {
		vStr := fmt.Sprintf("%v", v)
		if len(vStr) >= 2 && vStr[:2] == "$$" {
			localVar = vStr[2:]
		}
	}

	ea := emptyAggregator{
		key:        key,
		query:      config.Query,
		collection: config.Collection,
		database:   config.Database,
		localVar:   localVar,
	}
	switch config.Type {
	case "countAggregator":
		return &countAggregator{
			emptyAggregator: ea,
		}, nil
	case "valueAggregator":
		if config.Field == "" {
			return nil, fmt.Errorf("for field %v, 'field' can't be null or empty", key)
		}
		return &valueAggregator{
			emptyAggregator: ea,
			field:           config.Field,
		}, nil
	case "boundAggregator":
		if config.Field == "" {
			return nil, fmt.Errorf("for field %v, 'field' can't be null or empty", key)
		}
		return &boundAggregator{
			emptyAggregator: ea,
			field:           config.Field,
		}, nil
	default:
		return nil, fmt.Errorf("invalid type %v for field %v", config.Field, config.Type)
	}
}

// newAggregatorFromMap creates a slice of Aggregator based on a map of configuration
func (ci *CollInfo) newAggregatorFromMap(content map[string]Config) ([]Aggregator, error) {
	agArr := make([]Aggregator, 0)
	for k, v := range content {
		switch v.Type {
		case "countAggregator", "valueAggregator", "boundAggregator":
			a, err := ci.NewAggregator(k, &v)
			if err != nil {
				return nil, err
			}
			agArr = append(agArr, a)
		default:
		}
	}
	return agArr, nil
}

// AggregatorList creates a slice of Aggregator from a map of Config
func (ci *CollInfo) AggregatorList(content map[string]Config) ([]Aggregator, error) {
	return ci.newAggregatorFromMap(content)
}
