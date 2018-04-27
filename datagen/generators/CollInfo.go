package generators

import (
	"bytes"
	"fmt"
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
func NewCollInfo(count int, shortNames bool, version []int, seed uint64) *CollInfo {
	if count <= 0 {
		count = 1
	}
	return &CollInfo{
		Count:      count,
		ShortNames: shortNames,
		Version:    version,
		Seed:       seed,
		Encoder:    NewEncoder(),
		pcg32:      pcg.NewPCG32().Seed(seed, seed),
		pcg64:      pcg.NewPCG64().Seed(seed, seed, seed, seed),
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

// NewGenerator returns a new Generator based on config
func (ci *CollInfo) NewGenerator(key string, config *Config) (Generator, error) {
	if config.NullPercentage > 100 {
		return nil, fmt.Errorf("for field %s, null percentage can't be > 100", key)
	}
	// use a default key of length 1. This can happen for a generator of type fromArray
	// used as generator of an ArrayGenerator
	if len(key) == 0 {
		key = "k"
	}
	// EmptyGenerator to store general info
	eg := newEmptyGenerator(key, uint32(config.NullPercentage)*10, bson.ElementNil, ci.Encoder, ci.pcg32)

	if config.MaxDistinctValue != 0 {
		size := config.MaxDistinctValue
		config.MaxDistinctValue = 0
		arr, t, err := ci.preGenerate(key, config, size)
		if err != nil {
			return nil, err
		}
		eg.bsonType = t
		return &fromArrayGenerator{
			emptyGenerator: eg,
			array:          arr,
			size:           size,
			index:          0,
		}, nil
	}

	switch config.Type {
	case "string":
		if config.MinLength < 0 || config.MinLength > config.MaxLength {
			return nil, fmt.Errorf("for field %s, make sure that 'minLength' >= 0 and 'minLength' <= 'maxLength'", key)
		}
		eg.bsonType = bson.ElementString
		if config.Unique {
			// unique string can only be of fixed length, use maxLength as length
			u := &uniqueGenerator{
				currentIndex: 0,
			}
			err := u.getUniqueArray(ci.Count, int(config.MaxLength))
			if err != nil {
				return nil, fmt.Errorf("for field %s, %v", key, err)
			}
			return &fromArrayGenerator{
				emptyGenerator: eg,
				array:          u.values,
				size:           ci.Count,
				index:          0,
			}, nil
		}
		return &stringGenerator{
			emptyGenerator: eg,
			minLength:      uint32(config.MinLength),
			maxLength:      uint32(config.MaxLength),
		}, nil
	case "int":
		if config.MaxInt == 0 || config.MaxInt <= config.MinInt {
			return nil, fmt.Errorf("for field %s, make sure that 'maxInt' > 'minInt'", key)
		}
		eg.bsonType = bson.ElementInt32
		// Max = MaxInt + 1 so bound are inclusive
		return &int32Generator{
			emptyGenerator: eg,
			min:            config.MinInt,
			max:            config.MaxInt + 1,
		}, nil
	case "long":
		if config.MaxLong == 0 || config.MaxLong <= config.MinLong {
			return nil, fmt.Errorf("for field %s, make sure that 'maxLong' > 'minLong'", key)
		}
		eg.bsonType = bson.ElementInt64
		// Max = MaxLong + 1 so bound are inclusive
		return &int64Generator{
			emptyGenerator: eg,
			min:            config.MinLong,
			max:            config.MaxLong + 1,
			pcg64:          ci.pcg64,
		}, nil
	case "double":
		if config.MaxDouble == 0 || config.MaxDouble <= config.MinDouble {
			return nil, fmt.Errorf("for field %s, make sure that 'maxDouble' > 'minDouble'", key)
		}
		eg.bsonType = bson.ElementFloat64
		return &float64Generator{
			emptyGenerator: eg,
			mean:           config.MinDouble,
			stdDev:         (config.MaxDouble - config.MinDouble) / 2,
			pcg64:          ci.pcg64,
		}, nil
	case "decimal":
		if !ci.versionAtLeast(3, 4) {
			return nil, fmt.Errorf("for field %s, decimal type (bson decimal128) requires mongodb 3.4 at least", key)
		}
		eg.bsonType = bson.ElementDecimal128
		return &decimal128Generator{
			emptyGenerator: eg,
			pcg64:          ci.pcg64,
		}, nil
	case "boolean":
		eg.bsonType = bson.ElementBool
		return &boolGenerator{
			emptyGenerator: eg,
		}, nil
	case "objectId":
		eg.bsonType = bson.ElementObjectId
		return &objectIDGenerator{
			emptyGenerator: eg,
		}, nil
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

		eg.bsonType = bson.ElementArray
		return &arrayGenerator{
			emptyGenerator: eg,
			size:           config.Size,
			generator:      g,
		}, nil
	case "object":
		g, err := ci.newGeneratorsFromMap(config.ObjectContent)
		if err != nil {
			return nil, err
		}
		eg.bsonType = bson.ElementDocument
		return &embeddedObjectGenerator{
			emptyGenerator: eg,
			generators:     g,
		}, nil
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
			array[i] = raw[4 : len(raw)-1]
		}
		eg.bsonType = bson.ElementNil
		return &fromArrayGenerator{
			emptyGenerator: eg,
			array:          array,
			size:           len(config.In),
			index:          0,
		}, nil
	case "binary":
		if config.MinLength < 0 || config.MinLength > config.MaxLength {
			return nil, fmt.Errorf("for field %s, make sure that 'minLength' >= 0 and 'minLength' < 'maxLength'", key)
		}
		eg.bsonType = bson.ElementBinary
		return &binaryDataGenerator{
			emptyGenerator: eg,
			maxLength:      uint32(config.MaxLength),
			minLength:      uint32(config.MinLength),
		}, nil
	case "date":
		if config.StartDate.Unix() > config.EndDate.Unix() {
			return nil, fmt.Errorf("for field %s, make sure that 'startDate' < 'endDate'", key)
		}
		eg.bsonType = bson.ElementDatetime
		return &dateGenerator{
			emptyGenerator: eg,
			startDate:      uint64(config.StartDate.Unix()),
			delta:          uint64(config.EndDate.Unix() - config.StartDate.Unix()),
			pcg64:          ci.pcg64,
		}, nil
	case "position":
		eg.bsonType = bson.ElementArray
		return &positionGenerator{
			emptyGenerator: eg,
			pcg64:          ci.pcg64,
		}, nil
	case "constant":
		m := bson.M{key: config.ConstVal}
		raw, err := bson.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("for field %s, couldn't marshal value: %v", key, err)
		}
		eg.bsonType = bson.ElementNil
		return &constGenerator{
			emptyGenerator: eg,
			val:            raw[4 : len(raw)-1],
		}, nil
	case "autoincrement":
		switch config.AutoType {
		case "int":
			eg.bsonType = bson.ElementInt32
			return &autoIncrementGenerator32{
				emptyGenerator: eg,
				counter:        config.StartInt,
			}, nil
		case "long":
			eg.bsonType = bson.ElementInt64
			return &autoIncrementGenerator64{
				emptyGenerator: eg,
				counter:        config.StartLong,
			}, nil
		default:
			return nil, fmt.Errorf("invalid type %v for field %v", config.Type, key)
		}
	case "faker":
		eg.bsonType = bson.ElementString
		// use "en" lolcale for now, but should be configurable
		fk, err := faker.New("en")
		if err != nil {
			return nil, fmt.Errorf("fail to instantiate faker generator: %v", err)
		}
		var method func(f *faker.Faker) string
		switch config.Method {
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
		case "CompanyCatchPhrase":
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
		case "PostCode":
			method = (*faker.Faker).PostCode
		case "SafeEmail":
			method = (*faker.Faker).SafeEmail
		case "SecondaryAddress":
			method = (*faker.Faker).SecondaryAddress
		case "State":
			method = (*faker.Faker).State
		case "StateAbbr":
			method = (*faker.Faker).StateAbbr
		case "StreetAddress":
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
			return nil, fmt.Errorf("invalid Faker method for key %v: %v", key, config.Method)
		}
		return &fakerGenerator{
			emptyGenerator: eg,
			faker:          fk,
			f:              method,
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
		eg.bsonType = mapRefType[config.ID]
		return &fromArrayGenerator{
			emptyGenerator: eg,
			array:          mapRef[config.ID],
			size:           len(mapRef[config.ID]),
			index:          0,
			doNotTruncate:  true,
		}, nil
	case "countAggregator", "valueAggregator", "boundAggregator":
		return nil, nil
	default:
		return nil, fmt.Errorf("invalid type %v for field %v", config.Type, key)
	}
}

// newGeneratorsFromMap creates a slice of generators from a map of Config
func (ci *CollInfo) newGeneratorsFromMap(content map[string]Config) ([]Generator, error) {
	gArr := make([]Generator, 0)
	for k, v := range content {
		// if shortNames option is specified, keep only two letters for each field. This is a basic
		// optimisation to save space in mongodb and during db exchanges
		if ci.ShortNames && k != "_id" && len(k) > 2 {
			k = k[:2]
		}
		g, err := ci.NewGenerator(k, &v)
		if err != nil {
			return nil, err
		}
		if g != nil {
			gArr = append(gArr, g)
		}
	}
	return gArr, nil
}

// DocumentGenerator creates an object generator to generate valid bson documents
func (ci *CollInfo) DocumentGenerator(content map[string]Config) (Generator, error) {
	// create the global generator
	g, err := ci.newGeneratorsFromMap(content)
	if err != nil {
		return nil, fmt.Errorf("error while creating generators from configuration file:\n\tcause: %v", err)
	}
	return &objectGenerator{
		emptyGenerator: newEmptyGenerator("", 0, bson.ElementDocument, ci.Encoder, ci.pcg32),
		generators:     g,
	}, nil
}

// preGenerate generate `nb`values using a generator created from config
func (ci *CollInfo) preGenerate(key string, config *Config, nb int) ([][]byte, byte, error) {

	tmpCi := NewCollInfo(ci.Count, ci.ShortNames, ci.Version, ci.Seed)
	g, err := tmpCi.NewGenerator(key, config)
	if err != nil {
		return nil, bson.ElementNil, fmt.Errorf("for field %s, error while creating base array: %v", key, err)
	}

	arr := make([][]byte, nb)
	for i := 0; i < nb; i++ {
		g.Value()
		tmpArr := make([]byte, tmpCi.Encoder.Len())
		copy(tmpArr, tmpCi.Encoder.Bytes())
		arr[i] = tmpArr
		tmpCi.Encoder.Truncate(0)
	}
	if nb > 1 {
		if bytes.Equal(arr[0], arr[1]) {
			return nil, bson.ElementNil, fmt.Errorf("for field %s, couldn't generate enough unique values", key)
		}
	}
	return arr, g.Type(), nil
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
			if ci.ShortNames && k != "_id" && len(k) > 2 {
				k = k[:2]
			}
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

// DocumentAggregator creates a slice of Aggregator from a map of Config
func (ci *CollInfo) DocumentAggregator(content map[string]Config) ([]Aggregator, error) {
	return ci.newAggregatorFromMap(content)
}
