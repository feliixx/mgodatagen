package generators

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/MichaelTJones/pcg"
	"github.com/brianvoe/gofakeit/v6"
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
	// for `stringFromParts` type only. Generators used to create the string
	Parts []Config `json:"parts"`
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
	TypeString          = "string"
	TypeInt             = "int"
	TypeLong            = "long"
	TypeDouble          = "double"
	TypeDecimal         = "decimal"
	TypeBoolean         = "boolean"
	TypeObjectID        = "objectId"
	TypeArray           = "array"
	TypePosition        = "position"
	TypeObject          = "object"
	TypeFromArray       = "fromArray"
	TypeConstant        = "constant"
	TypeRef             = "ref"
	TypeAutoincrement   = "autoincrement"
	TypeBinary          = "binary"
	TypeDate            = "date"
	TypeUUID            = "uuid"
	TypeFaker           = "faker"
	TypeStringFromParts = "stringFromParts"
)

// available aggregator types
const (
	TypeCountAggregator = "countAggregator"
	TypeValueAggregator = "valueAggregator"
	TypeBoundAggregator = "boundAggregator"
)

// available faker methods
const (
	MethodAnimal                  = "Animal"
	MethodAnimalType              = "AnimalType"
	MethodBS                      = "BS"
	MethodBeerAlcohol             = "BeerAlcohol"
	MethodBeerBlg                 = "BeerBlg"
	MethodBeerHop                 = "BeerHop"
	MethodBeerIbu                 = "BeerIbu"
	MethodBeerMalt                = "BeerMalt"
	MethodBeerName                = "BeerName"
	MethodBeerStyle               = "BeerStyle"
	MethodBeerYeast               = "BeerYeast"
	MethodBuzzWord                = "BuzzWord"
	MethodCarMaker                = "CarMaker"
	MethodCarModel                = "CarModel"
	MethodCat                     = "Cat"
	MethodChromeUserAgent         = "ChromeUserAgent"
	MethodCity                    = "City"
	MethodColor                   = "Color"
	MethodCompany                 = "Company"
	MethodCompanySuffix           = "CompanySuffix"
	MethodCountry                 = "Country"
	MethodCountryAbr              = "CountryAbr"
	MethodCreditCardCvv           = "CreditCardCvv"
	MethodCreditCardExp           = "CreditCardExp"
	MethodCreditCardType          = "CreditCardType"
	MethodCurrencyLong            = "CurrencyLong"
	MethodCurrencyShort           = "CurrencyShort"
	MethodDog                     = "Dog"
	MethodDomainName              = "DomainName"
	MethodDomainSuffix            = "DomainSuffix"
	MethodEmail                   = "Email"
	MethodEmoji                   = "Emoji"
	MethodEmojiAlias              = "EmojiAlias"
	MethodEmojiCategory           = "EmojiCategory"
	MethodEmojiDescription        = "EmojiDescription"
	MethodEmojiTag                = "EmojiTag"
	MethodFileExtension           = "FileExtension"
	MethodFarmAnimal              = "FarmAnimal"
	MethodFirefoxUserAgent        = "FirefoxUserAgent"
	MethodFirstName               = "FirstName"
	MethodCarFuelType             = "CarFuelType"
	MethodGender                  = "Gender"
	MethodHTTPMethod              = "HTTPMethod"
	MethodHackerAbbreviation      = "HackerAbbreviation"
	MethodHackerAdjective         = "HackerAdjective"
	MethodHackeringVerb           = "HackeringVerb"
	MethodHackerNoun              = "HackerNoun"
	MethodHackerPhrase            = "HackerPhrase"
	MethodHackerVerb              = "HackerVerb"
	MethodHexColor                = "HexColor"
	MethodHipsterWord             = "HipsterWord"
	MethodIPv4Address             = "IPv4Address"
	MethodIPv6Address             = "IPv6Address"
	MethodJobDescriptor           = "JobDescriptor"
	MethodJobLevel                = "JobLevel"
	MethodJobTitle                = "JobTitle"
	MethodLanguage                = "Language"
	MethodLanguageAbbreviation    = "LanguageAbbreviation"
	MethodLastName                = "LastName"
	MethodLetter                  = "Letter"
	MethodMacAddress              = "MacAddress"
	MethodFileMimeType            = "FileMimeType"
	MethodMonth                   = "Month"
	MethodName                    = "Name"
	MethodNamePrefix              = "NamePrefix"
	MethodNameSuffix              = "NameSuffix"
	MethodOperaUserAgent          = "OperaUserAgent"
	MethodPetName                 = "PetName"
	MethodPhone                   = "Phone"
	MethodPhoneFormatted          = "PhoneFormatted"
	MethodProgrammingLanguage     = "ProgrammingLanguage"
	MethodProgrammingLanguageBest = "ProgrammingLanguageBest"
	MethodQuestion                = "Question"
	MethodQuote                   = "Quote"
	MethodSSN                     = "SSN"
	MethodSafariUserAgent         = "SafariUserAgent"
	MethodSafeColor               = "SafeColor"
	MethodState                   = "State"
	MethodStateAbr                = "StateAbr"
	MethodStreet                  = "Street"
	MethodStreetName              = "StreetName"
	MethodStreetNumber            = "StreetNumber"
	MethodStreetPrefix            = "StreetPrefix"
	MethodStreetSuffix            = "StreetSuffix"
	MethodTimeZone                = "TimeZone"
	MethodTimeZoneAbv             = "TimeZoneAbv"
	MethodTimeZoneFull            = "TimeZoneFull"
	MethodCarTransmissionType     = "CarTransmissionType"
	MethodURL                     = "URL"
	MethodUserAgent               = "UserAgent"
	MethodUsername                = "Username"
	MethodCarType                 = "CarType"
	MethodWeekDay                 = "WeekDay"
	MethodWord                    = "Word"
	MethodZip                     = "Zip"
)

var mapTypes = map[string]bsontype.Type{
	TypeString:          bson.TypeString,
	TypeInt:             bson.TypeInt32,
	TypeLong:            bson.TypeInt64,
	TypeDouble:          bson.TypeDouble,
	TypeDecimal:         bson.TypeDecimal128,
	TypeBoolean:         bson.TypeBoolean,
	TypeObjectID:        bson.TypeObjectID,
	TypeArray:           bson.TypeArray,
	TypePosition:        bson.TypeArray,
	TypeObject:          bson.TypeEmbeddedDocument,
	TypeFromArray:       bson.TypeNull, // can be of any bson type
	TypeConstant:        bson.TypeNull, // can be of any bson type
	TypeRef:             bson.TypeNull, // can be of any bson type
	TypeAutoincrement:   bson.TypeNull, // type bson.ElementInt32 or bson.ElementInt64
	TypeBinary:          bson.TypeBinary,
	TypeDate:            bson.TypeDateTime,
	TypeUUID:            bson.TypeString,
	TypeFaker:           bson.TypeString,
	TypeStringFromParts: bson.TypeString,

	TypeCountAggregator: bson.TypeNull,
	TypeValueAggregator: bson.TypeNull,
	TypeBoundAggregator: bson.TypeNull,
}

var fakerMethods = map[string]func() string{

	// Old manrevu/faker values are kept, but non documented
	// in order to avoid breaking previous working config
	// some names are exact duplicate of gofakeit methods, so
	// no need to add them to the list. Concerned method below:
	//
	// "CompanySuffix"
	// "Country"
	// "City"
	// "DomainName"
	// "DomainSuffix"
	// "Email"
	// "FirstName"
	// "JobTitle"
	// "LastName"
	// "Name"
	// "NamePrefix"
	// "NameSuffix"
	// "State"
	// "StreetName"
	// "StreetSuffix"
	// "URL"

	"CellPhoneNumber":    gofakeit.Phone,
	"CityPrefix":         gofakeit.City,
	"CitySuffix":         gofakeit.City,
	"CompanyBs":          gofakeit.BS,
	"CompanyCatchPhrase": gofakeit.HackerPhrase,
	"CompanyName":        gofakeit.Company,
	"DomainWord":         gofakeit.DomainName,
	"FreeEmail":          gofakeit.Email,
	"PhoneNumber":        gofakeit.Phone,
	"PostCode":           gofakeit.Zip,
	"SafeEmail":          gofakeit.Email,
	"SecondaryAddress":   gofakeit.StreetName,
	"StateAbbr":          gofakeit.StateAbr,
	"StreetAddress":      gofakeit.StreetName,
	"UserName":           gofakeit.Username,

	// old gofakeit method, kept for backawrd compatibility
	// but undocuemented

	"MimeType":             gofakeit.FileMimeType,
	"Extension":            gofakeit.FileExtension,
	"FuelType":             gofakeit.CarFuelType,
	"HackerIngverb":        gofakeit.HackerVerb,
	"MehtodStreet":         gofakeit.Street,
	"TransmissionGearType": gofakeit.CarTransmissionType,
	"VehicleType":          gofakeit.CarType,

	// current documented method

	MethodAnimal:                  gofakeit.Animal,
	MethodAnimalType:              gofakeit.AnimalType,
	MethodBS:                      gofakeit.BS,
	MethodBeerAlcohol:             gofakeit.BeerAlcohol,
	MethodBeerBlg:                 gofakeit.BeerBlg,
	MethodBeerHop:                 gofakeit.BeerHop,
	MethodBeerIbu:                 gofakeit.BeerIbu,
	MethodBeerMalt:                gofakeit.BeerMalt,
	MethodBeerName:                gofakeit.BeerName,
	MethodBeerStyle:               gofakeit.BeerStyle,
	MethodBeerYeast:               gofakeit.BeerYeast,
	MethodBuzzWord:                gofakeit.BuzzWord,
	MethodCarMaker:                gofakeit.CarMaker,
	MethodCarModel:                gofakeit.CarModel,
	MethodCat:                     gofakeit.Cat,
	MethodChromeUserAgent:         gofakeit.ChromeUserAgent,
	MethodColor:                   gofakeit.Color,
	MethodCity:                    gofakeit.City,
	MethodCompany:                 gofakeit.Company,
	MethodCompanySuffix:           gofakeit.CompanySuffix,
	MethodCountry:                 gofakeit.Country,
	MethodCountryAbr:              gofakeit.CountryAbr,
	MethodCreditCardCvv:           gofakeit.CreditCardCvv,
	MethodCreditCardExp:           gofakeit.CreditCardExp,
	MethodCreditCardType:          gofakeit.CreditCardType,
	MethodCurrencyLong:            gofakeit.CurrencyLong,
	MethodCurrencyShort:           gofakeit.CurrencyShort,
	MethodDog:                     gofakeit.Dog,
	MethodDomainName:              gofakeit.DomainName,
	MethodDomainSuffix:            gofakeit.DomainSuffix,
	MethodEmail:                   gofakeit.Email,
	MethodEmoji:                   gofakeit.Emoji,
	MethodEmojiAlias:              gofakeit.EmojiAlias,
	MethodEmojiCategory:           gofakeit.EmojiCategory,
	MethodEmojiDescription:        gofakeit.EmojiDescription,
	MethodEmojiTag:                gofakeit.EmojiTag,
	MethodFileExtension:           gofakeit.FileExtension,
	MethodFarmAnimal:              gofakeit.FarmAnimal,
	MethodFirefoxUserAgent:        gofakeit.FirefoxUserAgent,
	MethodFirstName:               gofakeit.FirstName,
	MethodCarFuelType:             gofakeit.CarFuelType,
	MethodGender:                  gofakeit.Gender,
	MethodHTTPMethod:              gofakeit.HTTPMethod,
	MethodHackerAbbreviation:      gofakeit.HackerAbbreviation,
	MethodHackerAdjective:         gofakeit.HackerAdjective,
	MethodHackeringVerb:           gofakeit.HackeringVerb,
	MethodHackerNoun:              gofakeit.HackerNoun,
	MethodHackerPhrase:            gofakeit.HackerPhrase,
	MethodHackerVerb:              gofakeit.HackerVerb,
	MethodHexColor:                gofakeit.HexColor,
	MethodHipsterWord:             gofakeit.HipsterWord,
	MethodIPv4Address:             gofakeit.IPv4Address,
	MethodIPv6Address:             gofakeit.IPv6Address,
	MethodJobDescriptor:           gofakeit.JobDescriptor,
	MethodJobLevel:                gofakeit.JobLevel,
	MethodJobTitle:                gofakeit.JobTitle,
	MethodLanguage:                gofakeit.Language,
	MethodLanguageAbbreviation:    gofakeit.LanguageAbbreviation,
	MethodLastName:                gofakeit.LastName,
	MethodLetter:                  gofakeit.Letter,
	MethodMacAddress:              gofakeit.MacAddress,
	MethodFileMimeType:            gofakeit.FileMimeType,
	MethodMonth:                   gofakeit.Month,
	MethodName:                    gofakeit.Name,
	MethodNamePrefix:              gofakeit.NamePrefix,
	MethodNameSuffix:              gofakeit.NameSuffix,
	MethodOperaUserAgent:          gofakeit.OperaUserAgent,
	MethodPetName:                 gofakeit.PetName,
	MethodPhone:                   gofakeit.Phone,
	MethodPhoneFormatted:          gofakeit.PhoneFormatted,
	MethodProgrammingLanguage:     gofakeit.ProgrammingLanguage,
	MethodProgrammingLanguageBest: gofakeit.ProgrammingLanguageBest,
	MethodQuestion:                gofakeit.Question,
	MethodQuote:                   gofakeit.Quote,
	MethodSSN:                     gofakeit.SSN,
	MethodSafariUserAgent:         gofakeit.SafariUserAgent,
	MethodSafeColor:               gofakeit.SafeColor,
	MethodState:                   gofakeit.State,
	MethodStateAbr:                gofakeit.StateAbr,
	MethodStreet:                  gofakeit.Street,
	MethodStreetName:              gofakeit.StreetName,
	MethodStreetNumber:            gofakeit.StreetNumber,
	MethodStreetPrefix:            gofakeit.StreetPrefix,
	MethodStreetSuffix:            gofakeit.StreetSuffix,
	MethodTimeZone:                gofakeit.TimeZone,
	MethodTimeZoneAbv:             gofakeit.TimeZoneAbv,
	MethodTimeZoneFull:            gofakeit.TimeZoneFull,
	MethodCarTransmissionType:     gofakeit.CarTransmissionType,
	MethodURL:                     gofakeit.URL,
	MethodUserAgent:               gofakeit.UserAgent,
	MethodUsername:                gofakeit.Username,
	MethodCarType:                 gofakeit.CarType,
	MethodWeekDay:                 gofakeit.WeekDay,
	MethodWord:                    gofakeit.Word,
	MethodZip:                     gofakeit.Zip,
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
			return nil, fmt.Errorf("invalid generator for field '%s'\n  cause: %v", k, err)
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
		return nil, fmt.Errorf("invalid type '%s'", config.Type)
	}
	nullPercentage := uint32(config.NullPercentage) * 10
	base := newBase(key, nullPercentage, bsonType, buffer, ci.pcg32)

	if config.MaxDistinctValue != 0 {
		// there is no point in having a maxDistinctValue
		// greater than the number of doc to generate, since
		// it use a fromArrayGenerator with randomOrder=false
		size := config.MaxDistinctValue
		if size > ci.Count {
			size = ci.Count
		}
		// set to 0 to avoid infinite loop when calling ci.preGenerate()
		config.MaxDistinctValue = 0

		values, bsonType, err := ci.preGenerate(key, config, size)
		if err != nil {
			return nil, err
		}
		base.bsonType = bsonType

		return newFromArrayGeneratorWithPregeneratedValues(base, values, false)
	}

	switch config.Type {

	case TypeString:
		return newStringGenerator(config, base, ci.Count)

	case TypeInt:
		return newIntGenerator(config, base)

	case TypeLong:
		return newLongGenerator(config, base, ci.pcg64)

	case TypeDouble:
		return newDoubleGenerator(config, base, ci.pcg64)

	case TypeDecimal:
		if !ci.versionAtLeast(3, 4) {
			return nil, errors.New("decimal type (bson decimal128) requires mongodb 3.4 at least")
		}
		return newDecimalGenerator(base, ci.pcg64)

	case TypeBoolean:
		return newBoolGenerator(base)

	case TypeObjectID:
		return newObjectIDGenerator(base)

	case TypeArray:
		return newArrayGenerator(config, base, ci, buffer)

	case TypeObject:
		return newEmbededGenerator(config, base, ci, buffer)

	case TypeFromArray:
		return newFromArrayGenerator(config, base)

	case TypeBinary:
		return newBinaryGenerator(config, base)

	case TypeDate:
		return newDateGenerator(config, base, ci.pcg64)

	case TypePosition:
		return newPositionGenerator(base, ci.pcg64)

	case TypeConstant:
		return newConstantGenerator(base, config.ConstVal)

	case TypeAutoincrement:
		switch config.AutoType {
		case TypeInt:
			return newAutoIncrementIntGenerator(config, base)
		case TypeLong:
			return newAutoIncrementLongGenerator(config, base)
		default:
			return nil, fmt.Errorf("invalid type '%s'", config.Type)
		}

	case TypeUUID:
		return newUUIDGenerator(base)

	case TypeFaker:
		return newFakerGenerator(config, base)

	case TypeStringFromParts:
		return newStringFromPartsGenerator(config, base, ci, buffer)

	case TypeRef:
		_, ok := ci.mapRef[config.ID]
		if !ok {

			if config.RefContent == nil {
				return nil, errors.New("'refContent' can't be null or empty'")
			}

			values, bsonType, err := ci.preGenerate(key, config.RefContent, ci.Count)
			if err != nil {
				return nil, err
			}
			ci.mapRef[config.ID] = values
			ci.mapRefType[config.ID] = bsonType
		}
		base.bsonType = ci.mapRefType[config.ID]
		return newFromArrayGeneratorWithPregeneratedValues(base, ci.mapRef[config.ID], true)
	}

	return nil, nil
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
		g.EncodeValue()
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
				return nil, fmt.Errorf("invalid generator for field '%s'\n  cause: %v", k, err)
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
