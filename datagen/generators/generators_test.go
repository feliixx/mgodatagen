package generators_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/feliixx/mgodatagen/datagen/generators"
)

const defaultSeed = 0

func TestIsDocumentCorrect(t *testing.T) {

	var expectedDoc struct {
		ID                      primitive.ObjectID `bson:"_id"`
		UUID                    string             `bson:"uuid"`
		String                  string             `bson:"string"`
		Int32                   int32              `bson:"int32"`
		Int64                   int64              `bson:"int64"`
		Float                   float64            `bson:"float"`
		ConstInt32              int32              `bson:"constInt32"`
		ConstInt64              int64              `bson:"constInt64"`
		ConstFloat              float64            `bson:"constFloat"`
		Boolean                 bool               `bson:"boolean"`
		Position                []float64          `bson:"position"`
		StringFromArray         string             `bson:"stringFromArray"`
		IntFromArrayRandomOrder int                `bson:"intFromArrayRandomOrder"`
		ArrayFromArray          []string           `bson:"arrayFromArray"`
		ConstArray              []string           `bson:"constArray"`
		Fake                    string             `bson:"faker"`
		Constant                int32              `bson:"constant"`
		AutoIncrementInt32      int32              `bson:"autoIncrementInt32"`
		AutoIncrementInt64      int64              `bson:"autoIncrementInt64"`
		Date                    time.Time          `bson:"date"`
		BinaryData              []byte             `bson:"binaryData"`
		ArrayInt32              []int32            `bson:"arrayInt32"`
		Object                  struct {
			K1    string `bson:"k1"`
			K2    int32  `bson:"k2"`
			Subob struct {
				Sk int32 `bson:"s-k"`
			} `bson:"sub-ob"`
		} `bson:"object"`
		StringFromParts string `bson:"stringFromParts"`
	}

	fullDocumentTests := []struct {
		name     string
		content  map[string]generators.Config
		expected interface{}
	}{
		{
			name:     "full-bson.json",
			content:  loadCollConfig(t, "full-bson.json")[0],
			expected: expectedDoc,
		},
		{
			name:     "empty generator",
			content:  map[string]generators.Config{},
			expected: bson.M{},
		},
	}

	ci := generators.NewCollInfo(1000, []int{3, 2}, defaultSeed, map[int][][]byte{}, map[int]bsontype.Type{})

	for _, tt := range fullDocumentTests {
		t.Run(tt.name, func(t *testing.T) {
			docGenerator, err := ci.NewDocumentGenerator(tt.content)
			if err != nil {
				t.Error(err)
			}
			for i := 0; i < ci.Count; i++ {
				err := bson.Unmarshal(docGenerator.Generate(), &tt.expected)
				if err != nil {
					t.Errorf("fail to unmarshal doc: %v", err)
				}
			}
		})
	}
}

func TestBigArray(t *testing.T) {

	ci := generators.NewCollInfo(-1, []int{3, 6, 4}, defaultSeed, nil, nil)
	docGenerator, err := ci.NewDocumentGenerator(map[string]generators.Config{
		"key": {Type: generators.TypeArray, Size: 15, ArrayContent: &generators.Config{Type: generators.TypeBoolean}},
	})
	if err != nil {
		t.Error(err)
	}

	var a struct {
		Key []bool `bson:"key"`
	}
	for i := 0; i < 100; i++ {
		err := bson.Unmarshal(docGenerator.Generate(), &a)
		if err != nil {
			t.Error(err)
		}
		if want, got := 15, len(a.Key); want != got {
			t.Errorf("wrong array size, expected %d, got %d", want, got)
		}
	}
}

func TestDocumentWithDecimal128(t *testing.T) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed, nil, nil)
	docGenerator, err := ci.NewDocumentGenerator(map[string]generators.Config{
		"key": {Type: generators.TypeDecimal},
	})
	if err != nil {
		t.Error(err)
	}

	var d struct {
		Decimal primitive.Decimal128 `bson:"decimal"`
	}
	for i := 0; i < 1000; i++ {
		err := bson.Unmarshal(docGenerator.Generate(), &d)
		if err != nil {
			t.Error(err)
		}
	}
}

func TestNewGenerator(t *testing.T) {
	type testCase struct {
		name    string
		config  generators.Config
		correct bool
		version []int
	}

	newGeneratorTests := []testCase{
		{
			name: "string invalid minLength",
			config: generators.Config{
				Type:      generators.TypeString,
				MinLength: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "string invalid maxLength",
			config: generators.Config{
				Type:      generators.TypeString,
				MinLength: 5,
				MaxLength: 2,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "unique with length == 0",
			config: generators.Config{
				Type:      generators.TypeString,
				MinLength: 0,
				Unique:    true,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "unique with string size to low",
			config: generators.Config{
				Type:      generators.TypeString,
				MinLength: 1,
				MaxLength: 1,
				Unique:    true,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "maxDistinctValue too high",
			config: generators.Config{
				Type:             generators.TypeString,
				MinLength:        0,
				MaxDistinctValue: 10,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "int with missing MinInt and MaxInt",
			config: generators.Config{
				Type: generators.TypeInt,
			},
			correct: true,
			version: []int{3, 6},
		},
		{
			name: "int with MaxInt < MinInt",
			config: generators.Config{
				Type:   generators.TypeInt,
				MinInt: 10,
				MaxInt: 4,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "long with missing MinLong and MaxLong",
			config: generators.Config{
				Type: generators.TypeLong,
			},
			correct: true,
			version: []int{3, 6},
		},
		{
			name: "long with MaxLong < MinLong",
			config: generators.Config{
				Type:    generators.TypeLong,
				MinLong: 10,
				MaxLong: 4,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "double with missing MinDouble and MaxDouble",
			config: generators.Config{
				Type: generators.TypeDouble,
			},
			correct: true,
			version: []int{3, 6},
		},
		{
			name: "double with MaxFloat < MinFloat",
			config: generators.Config{
				Type:      generators.TypeDouble,
				MinDouble: 10,
				MaxDouble: 4,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "array with size < 0 ",
			config: generators.Config{
				Type: generators.TypeArray,
				Size: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "array with invalid content",
			config: generators.Config{
				Type: generators.TypeArray,
				Size: 3,
				ArrayContent: &generators.Config{
					Type:      generators.TypeString,
					MinLength: -1,
				},
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "empty fromArray",
			config: generators.Config{
				Type: generators.TypeFromArray,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "binary with invalid minLength",
			config: generators.Config{
				Type:      generators.TypeBinary,
				MinLength: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "binary with incorrect MaxLength",
			config: generators.Config{
				Type:      generators.TypeBinary,
				MinLength: 5,
				MaxLength: 2,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "date with incorrect bounds",
			config: generators.Config{
				Type:      generators.TypeDate,
				StartDate: time.Now(),
				EndDate:   time.Unix(10, 10),
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "autoincrement generator with no type specified",
			config: generators.Config{
				Type:     generators.TypeAutoincrement,
				AutoType: "",
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "ref generator with invalid generator",
			config: generators.Config{
				Type: generators.TypeRef,
				RefContent: &generators.Config{
					Type:      generators.TypeString,
					MinLength: -1,
				},
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "ref generator with no generator",
			config: generators.Config{
				Type: generators.TypeRef,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "object generator with invalid generator",
			config: generators.Config{
				Type: generators.TypeObject,
				ObjectContent: map[string]generators.Config{
					"key": {
						Type:      generators.TypeString,
						MinLength: -1,
					},
				},
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name:    "missing type",
			config:  generators.Config{},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "null percentage > 100",
			config: generators.Config{
				Type:           generators.TypeString,
				NullPercentage: 120,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "unknown faker val",
			config: generators.Config{
				Type:   generators.TypeFaker,
				Method: "unknown",
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "type aggregator",
			config: generators.Config{
				Type:           generators.TypeCountAggregator,
				NullPercentage: 10,
			},
			correct: true,
			version: []int{3, 6},
		},
		{
			name: "unknown type",
			config: generators.Config{
				Type: "unknown",
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "decimal with mongodb 3.2",
			config: generators.Config{
				Type: generators.TypeDecimal,
			},
			correct: false,
			version: []int{3, 2, 1},
		},
		{
			name: "decimal with mongodb 3.6",
			config: generators.Config{
				Type: generators.TypeDecimal,
			},
			correct: true,
			version: []int{3, 4},
		},
		{
			name: "negative maxDistinctValue",
			config: generators.Config{
				Type:             generators.TypeBoolean,
				MaxDistinctValue: -1,
			},
			correct: false,
			version: []int{3, 4},
		},
		{
			name: "constant with invalid value",
			config: generators.Config{
				Type:     generators.TypeConstant,
				ConstVal: bson.Raw("hjkgkgkg"),
			},
			correct: false,
			version: []int{3, 4},
		},
		{
			name: "max distinct value > coll.Count",
			config: generators.Config{
				Type:             generators.TypePosition,
				MaxDistinctValue: 101,
			},
			correct: true,
			version: []int{4},
		},
		{
			name: "stringFromParts generator with no generators",
			config: generators.Config{
				Type: generators.TypeStringFromParts,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "stringFromParts generator with invalid generator",
			config: generators.Config{
				Type: generators.TypeStringFromParts,
				Parts: []generators.Config{
					{
						Type:      generators.TypeString,
						MinLength: -1,
					},
				},
			},
			correct: false,
			version: []int{3, 6},
		},
	}
	// all possible faker methods
	fakerVal := []string{
		"CellPhoneNumber",
		"CityPrefix",
		"CitySuffix",
		"CompanyBs",
		"CompanyCatchPhrase",
		"CompanyName",
		"DomainWord",
		"FreeEmail",
		"PhoneNumber",
		"PostCode",
		"SafeEmail",
		"SecondaryAddress",
		"StateAbbr",
		"StreetAddress",
		"UserName",
		generators.MethodAnimal,
		generators.MethodAnimalType,
		generators.MethodBS,
		generators.MethodBeerAlcohol,
		generators.MethodBeerBlg,
		generators.MethodBeerHop,
		generators.MethodBeerIbu,
		generators.MethodBeerMalt,
		generators.MethodBeerName,
		generators.MethodBeerStyle,
		generators.MethodBeerYeast,
		generators.MethodBuzzWord,
		generators.MethodCarMaker,
		generators.MethodCarModel,
		generators.MethodCat,
		generators.MethodChromeUserAgent,
		generators.MethodColor,
		generators.MethodCity,
		generators.MethodCompany,
		generators.MethodCompanySuffix,
		generators.MethodCountry,
		generators.MethodCountryAbr,
		generators.MethodCreditCardCvv,
		generators.MethodCreditCardExp,
		generators.MethodCreditCardType,
		generators.MethodCurrencyLong,
		generators.MethodCurrencyShort,
		generators.MethodDog,
		generators.MethodDomainName,
		generators.MethodDomainSuffix,
		generators.MethodEmail,
		generators.MethodEmoji,
		generators.MethodEmojiAlias,
		generators.MethodEmojiCategory,
		generators.MethodEmojiDescription,
		generators.MethodEmojiTag,
		generators.MethodFileExtension,
		generators.MethodFarmAnimal,
		generators.MethodFirefoxUserAgent,
		generators.MethodFirstName,
		generators.MethodCarFuelType,
		generators.MethodGender,
		generators.MethodHTTPMethod,
		generators.MethodHackerAbbreviation,
		generators.MethodHackerAdjective,
		generators.MethodHackeringVerb,
		generators.MethodHackerNoun,
		generators.MethodHackerPhrase,
		generators.MethodHackerVerb,
		generators.MethodHexColor,
		generators.MethodHipsterWord,
		generators.MethodIPv4Address,
		generators.MethodIPv6Address,
		generators.MethodJobDescriptor,
		generators.MethodJobLevel,
		generators.MethodJobTitle,
		generators.MethodLanguage,
		generators.MethodLanguageAbbreviation,
		generators.MethodLastName,
		generators.MethodLetter,
		generators.MethodMacAddress,
		generators.MethodFileMimeType,
		generators.MethodMonth,
		generators.MethodName,
		generators.MethodNamePrefix,
		generators.MethodNameSuffix,
		generators.MethodOperaUserAgent,
		generators.MethodPetName,
		generators.MethodPhone,
		generators.MethodPhoneFormatted,
		generators.MethodProgrammingLanguage,
		generators.MethodProgrammingLanguageBest,
		generators.MethodQuestion,
		generators.MethodQuote,
		generators.MethodSSN,
		generators.MethodSafariUserAgent,
		generators.MethodSafeColor,
		generators.MethodState,
		generators.MethodStateAbr,
		generators.MethodStreet,
		generators.MethodStreetName,
		generators.MethodStreetNumber,
		generators.MethodStreetPrefix,
		generators.MethodStreetSuffix,
		generators.MethodTimeZone,
		generators.MethodTimeZoneAbv,
		generators.MethodTimeZoneFull,
		generators.MethodCarTransmissionType,
		generators.MethodURL,
		generators.MethodUserAgent,
		generators.MethodUsername,
		generators.MethodCarType,
		generators.MethodWeekDay,
		generators.MethodWord,
		generators.MethodZip,
	}

	for _, f := range fakerVal {
		newGeneratorTests = append(newGeneratorTests, testCase{
			name: fmt.Sprintf(`faker generator with method "%s"`, f),
			config: generators.Config{
				Type:   generators.TypeFaker,
				Method: f,
			},
			correct: true,
			version: []int{3, 6},
		})
	}

	ci := generators.NewCollInfo(100, nil, defaultSeed, map[int][][]byte{}, map[int]bsontype.Type{})

	for _, tt := range newGeneratorTests {
		t.Run(tt.name, func(t *testing.T) {
			ci.Version = tt.version
			var content = map[string]generators.Config{
				"k": tt.config,
			}
			_, err := ci.NewDocumentGenerator(content)
			if tt.correct && err != nil {
				t.Errorf("expected no error for generator with config %v \nbut got \n%v", tt.config, err)
			}
			if !tt.correct && err == nil {
				t.Errorf("generator with config %v should fail", tt.config)
			}
		})
	}
}

func TestNewGeneratorFromMap(t *testing.T) {

	contentList := loadCollConfig(t, "ref.json")

	generatorFromMapTests := []struct {
		name         string
		config       map[string]generators.Config
		correct      bool
		nbGenerators int
	}{
		{
			name: "invalid generator",
			config: map[string]generators.Config{
				"key": {
					Type:      generators.TypeString,
					MinLength: -1,
				},
			},
			correct:      false,
			nbGenerators: 0,
		}, {
			name:         "ref.json[0]",
			config:       contentList[0],
			correct:      true,
			nbGenerators: 14,
		}, {
			name:         "ref.json[1]",
			config:       contentList[1],
			correct:      true,
			nbGenerators: 3,
		},
	}

	ci := generators.NewCollInfo(1, []int{3, 4}, defaultSeed, map[int][][]byte{}, map[int]bsontype.Type{})

	for _, tt := range generatorFromMapTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ci.NewDocumentGenerator(tt.config)
			if tt.correct {
				if err != nil {
					t.Errorf("expected no error for config %v \n%v", tt.config, err)
				}
			} else if !tt.correct && err == nil {
				t.Errorf("expected an error for config %v but got none", tt.config)
			}
		})
	}
}

func loadCollConfig(t *testing.T, filename string) []map[string]generators.Config {
	bytes, err := ioutil.ReadFile("testdata/" + filename)
	if err != nil {
		t.Error(err)
	}
	var cc []struct {
		Content map[string]generators.Config `json:"content"`
	}
	err = json.Unmarshal(bytes, &cc)
	if err != nil {
		t.Error(err)
	}
	list := make([]map[string]generators.Config, 0, len(cc))
	for _, c := range cc {
		list = append(list, c.Content)
	}
	return list
}

func BenchmarkGeneratorAll(b *testing.B) {

	contentList := loadCollConfig(nil, "part.json")

	ci := generators.NewCollInfo(1000, []int{3, 2}, defaultSeed, map[int][][]byte{}, map[int]bsontype.Type{})
	docGenerator, err := ci.NewDocumentGenerator(contentList[0])
	if err != nil {
		b.Fail()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		docGenerator.Generate()
	}
}
