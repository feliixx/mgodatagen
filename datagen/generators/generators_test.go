package generators_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/globalsign/mgo/bson"

	"github.com/feliixx/mgodatagen/datagen/generators"
)

const defaultSeed = 0

func TestIsDocumentCorrect(t *testing.T) {

	contentList := loadCollConfig(t, "full-bson.json")
	ci := generators.NewCollInfo(1000, []int{3, 2}, defaultSeed)
	generator, err := ci.DocumentGenerator(contentList[0])
	if err != nil {
		t.Error(err)
	}

	var expectedDoc struct {
		ID         bson.ObjectId `bson:"_id"`
		Name       string        `bson:"name"`
		C32        int32         `bson:"c32"`
		C64        int64         `bson:"c64"`
		Float      float64       `bson:"float"`
		Verified   bool          `bson:"verified"`
		Position   []float64     `bson:"position"`
		Dt         string        `bson:"dt"`
		Afa        []string      `bson:"afa"`
		Fake       string        `bson:"faker"`
		Cst        int32         `bson:"cst"`
		Nb         int64         `bson:"nb"`
		Nnb        int32         `bson:"nnb"`
		Date       time.Time     `bson:"date"`
		BinaryData []byte        `bson:"binaryData"`
		List       []int32       `bson:"list"`
		Object     struct {
			K1    string `bson:"k1"`
			K2    int32  `bson:"k2"`
			Subob struct {
				Sk int32 `bson:"s-k"`
			} `bson:"sub-ob"`
		} `bson:"object"`
	}

	for i := 0; i < 1000; i++ {
		generator.Value()
		err := bson.Unmarshal(ci.Encoder.Bytes(), &expectedDoc)
		if err != nil {
			t.Errorf("fail to unmarshal doc: %v", err)
		}
	}
}

func TestBigArray(t *testing.T) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed)
	docGenerator, err := ci.DocumentGenerator(map[string]generators.Config{
		"key": {Type: "array", Size: 15, ArrayContent: &generators.Config{Type: "boolean"}},
	})
	if err != nil {
		t.Error(err)
	}

	var a struct {
		Key []bool `bson:"key"`
	}
	for i := 0; i < 100; i++ {
		docGenerator.Value()
		err := bson.Unmarshal(ci.Encoder.Bytes(), &a)
		if err != nil {
			t.Error(err)
		}
		if want, got := 15, len(a.Key); want != got {
			t.Errorf("wrong array size, expected %d, got %d", want, got)
		}
	}
}

func TestDocumentWithDecimal128(t *testing.T) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed)
	docGenerator, err := ci.DocumentGenerator(map[string]generators.Config{
		"key": {Type: "decimal"},
	})
	if err != nil {
		t.Error(err)
	}

	var d struct {
		Decimal bson.Decimal128 `bson:"decimal"`
	}
	for i := 0; i < 1000; i++ {
		docGenerator.Value()
		err := bson.Unmarshal(ci.Encoder.Bytes(), &d)
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
				Type:      "string",
				MinLength: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "string invalid maxLength",
			config: generators.Config{
				Type:      "string",
				MinLength: 5,
				MaxLength: 2,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "unique with length == 0",
			config: generators.Config{
				Type:      "string",
				MinLength: 0,
				Unique:    true,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "unique with string size to low",
			config: generators.Config{
				Type:      "string",
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
				Type:             "string",
				MinLength:        0,
				MaxDistinctValue: 10,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "int with invalid minInt32",
			config: generators.Config{
				Type:   "int",
				MinInt: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "int with invalid maxInt32",
			config: generators.Config{
				Type:   "int",
				MinInt: 10,
				MaxInt: 4,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "long with invalid minInt64",
			config: generators.Config{
				Type:    "long",
				MinLong: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "long with invalid MaxInt64",
			config: generators.Config{
				Type:    "long",
				MinLong: 10,
				MaxLong: 4,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "double with invalid minFloat",
			config: generators.Config{
				Type:      "double",
				MinDouble: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "double with invalid MaxFloat",
			config: generators.Config{
				Type:      "double",
				MinDouble: 10,
				MaxDouble: 4,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "array with size < 0 ",
			config: generators.Config{
				Type: "array",
				Size: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "array with invalid content",
			config: generators.Config{
				Type: "array",
				Size: 3,
				ArrayContent: &generators.Config{
					Type:      "string",
					MinLength: -1,
				},
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "empty fromArray",
			config: generators.Config{
				Type: "fromArray",
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "fromArray with invalid BSON values",
			config: generators.Config{
				Type: "fromArray",
				In: []interface{}{
					bson.M{
						"_id": bson.ObjectId("aaaa"),
					},
				},
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "binary with invalid minLength",
			config: generators.Config{
				Type:      "binary",
				MinLength: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "binary with incorrect MaxLength",
			config: generators.Config{
				Type:      "binary",
				MinLength: 5,
				MaxLength: 2,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "date with incorrect bounds",
			config: generators.Config{
				Type:      "date",
				StartDate: time.Now(),
				EndDate:   time.Unix(10, 10),
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "constant with invalid BSON value",
			config: generators.Config{
				Type: "constant",
				ConstVal: bson.M{
					"_id": bson.ObjectId("aaaa"),
				},
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "autoincrement generator with no type specified",
			config: generators.Config{
				Type:     "autoincrement",
				AutoType: "",
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "ref generator with invalid generator",
			config: generators.Config{
				Type: "ref",
				RefContent: &generators.Config{
					Type:      "string",
					MinLength: -1,
				},
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "object generator with invalid generator",
			config: generators.Config{
				Type: "object",
				ObjectContent: map[string]generators.Config{
					"key": {
						Type:      "string",
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
				Type:           "string",
				NullPercentage: 120,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "unknown faker val",
			config: generators.Config{
				Type:   "faker",
				Method: "unknown",
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "type aggregator",
			config: generators.Config{
				Type:           "countAggregator",
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
				Type: "decimal",
			},
			correct: false,
			version: []int{3, 2},
		},
		{
			name: "decimal with mongodb 3.6",
			config: generators.Config{
				Type: "decimal",
			},
			correct: true,
			version: []int{3, 4},
		},
	}
	// all possible faker methods
	fakerVal := []string{
		"CellPhoneNumber",
		"City",
		"CityPrefix",
		"CitySuffix",
		"CompanyBs",
		"CompanyCatchPhrase",
		"CompanyName",
		"CompanySuffix",
		"Country",
		"DomainName",
		"DomainSuffix",
		"DomainWord",
		"Email",
		"FirstName",
		"FreeEmail",
		"JobTitle",
		"LastName",
		"Name",
		"NamePrefix",
		"NameSuffix",
		"PhoneNumber",
		"PostCode",
		"SafeEmail",
		"SecondaryAddress",
		"State",
		"StateAbbr",
		"StreetAddress",
		"StreetName",
		"StreetSuffix",
		"URL",
		"UserName",
	}

	for _, f := range fakerVal {
		newGeneratorTests = append(newGeneratorTests, testCase{
			name: fmt.Sprintf(`faker generator with method "%s"`, f),
			config: generators.Config{
				Type:   "faker",
				Method: f,
			},
			correct: true,
			version: []int{3, 6},
		})
	}

	ci := generators.NewCollInfo(100, nil, defaultSeed)

	for _, tt := range newGeneratorTests {
		t.Run(tt.name, func(t *testing.T) {
			ci.Version = tt.version
			_, err := ci.NewGenerator("k", &tt.config)
			if tt.correct && err != nil {
				t.Errorf("expected no error for generator with config %v \nbut got \n%v", tt.config, err)
			}
			if !tt.correct && err == nil {
				t.Errorf("expected no error for generator with config %v", tt.config)
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
					Type:      "string",
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

	ci := generators.NewCollInfo(1, []int{3, 4}, defaultSeed)

	for _, tt := range generatorFromMapTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ci.DocumentGenerator(tt.config)
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
func BenchmarkGeneratorString(b *testing.B) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed)
	stringGenerator, err := ci.NewGenerator("key", &generators.Config{
		Type:      "string",
		MinLength: 5,
		MaxLength: 8,
	})
	if err != nil {
		b.Fail()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stringGenerator.Value()
		ci.Encoder.Truncate(0)
	}
}
func BenchmarkGeneratorInt32(b *testing.B) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed)
	int32Generator, err := ci.NewGenerator("key", &generators.Config{
		Type:   "int",
		MinInt: 0,
		MaxInt: 100,
	})
	if err != nil {
		b.Fail()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		int32Generator.Value()
		ci.Encoder.Truncate(0)
	}
}
func BenchmarkGeneratorInt64(b *testing.B) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed)
	int64Generator, err := ci.NewGenerator("key", &generators.Config{
		Type:    "long",
		MinLong: 0,
		MaxLong: 100,
	})
	if err != nil {
		b.Fail()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		int64Generator.Value()
		ci.Encoder.Truncate(0)
	}
}
func BenchmarkGeneratorFloat64(b *testing.B) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed)
	float64Generator, err := ci.NewGenerator("key", &generators.Config{
		Type:      "double",
		MinDouble: 0,
		MaxDouble: 100,
	})
	if err != nil {
		b.Fail()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		float64Generator.Value()
		ci.Encoder.Truncate(0)
	}
}
func BenchmarkGeneratorBool(b *testing.B) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed)
	boolGenerator, err := ci.NewGenerator("key", &generators.Config{
		Type: "boolean",
	})
	if err != nil {
		b.Fail()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		boolGenerator.Value()
		ci.Encoder.Truncate(0)
	}
}

func BenchmarkGeneratorPos(b *testing.B) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed)
	posGenerator, err := ci.NewGenerator("key", &generators.Config{
		Type: "position",
	})
	if err != nil {
		b.Fail()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		posGenerator.Value()
		ci.Encoder.Truncate(0)
	}
}

func BenchmarkGeneratorObjectId(b *testing.B) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed)
	objectIDGenerator, err := ci.NewGenerator("key", &generators.Config{
		Type: "objectId",
	})
	if err != nil {
		b.Fail()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		objectIDGenerator.Value()
		ci.Encoder.Truncate(0)
	}
}

func BenchmarkGeneratorBinary(b *testing.B) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed)
	binaryGenerator, err := ci.NewGenerator("key", &generators.Config{
		Type:      "binary",
		MinLength: 20,
		MaxLength: 40,
	})
	if err != nil {
		b.Fail()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binaryGenerator.Value()
		ci.Encoder.Truncate(0)
	}
}
func BenchmarkGeneratorDecimal128(b *testing.B) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed)
	decimal128Generator, err := ci.NewGenerator("key", &generators.Config{
		Type: "decimal",
	})
	if err != nil {
		b.Fail()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decimal128Generator.Value()
		ci.Encoder.Truncate(0)
	}
}
func BenchmarkGeneratorDate(b *testing.B) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed)
	dateGenerator, err := ci.NewGenerator("key", &generators.Config{
		Type:      "decimal",
		StartDate: time.Now(),
		EndDate:   time.Now().Add(7 * 24 * time.Hour),
	})
	if err != nil {
		b.Fail()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dateGenerator.Value()
		ci.Encoder.Truncate(0)
	}
}

func BenchmarkGeneratorArray(b *testing.B) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed)
	arrayGenerator, err := ci.NewGenerator("key", &generators.Config{
		Type:         "array",
		Size:         5,
		ArrayContent: &generators.Config{Type: "boolean"},
	})
	if err != nil {
		b.Fail()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arrayGenerator.Value()
		ci.Encoder.Truncate(0)
	}
}

func BenchmarkGeneratorAll(b *testing.B) {

	contentList := loadCollConfig(nil, "ref.json")

	ci := generators.NewCollInfo(1000, []int{3, 2}, defaultSeed)
	docGenerator, err := ci.DocumentGenerator(contentList[0])
	if err != nil {
		b.Fail()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		docGenerator.Value()
	}
}
