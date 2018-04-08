package generators

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/globalsign/mgo/bson"

	"github.com/feliixx/mgodatagen/config"
)

const defaultSeed = 0

type expectedDoc struct {
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

func TestIsDocumentCorrect(t *testing.T) {
	content, err := ioutil.ReadFile("../samples/bson_test.json")
	if err != nil {
		t.Fail()
	}
	collectionList, err := config.ParseConfig(content, false)
	if err != nil {
		t.Error(err)
	}

	ci := NewCollInfo(1000, false, []int{3, 2}, defaultSeed)

	generator, err := ci.DocumentGenerator(collectionList[0].Content)
	if err != nil {
		t.Error(err)
	}

	var d expectedDoc
	for i := 0; i < 1000; i++ {
		generator.Value()
		err := bson.Unmarshal(ci.Encoder.Data, &d)
		if err != nil {
			t.Errorf("fail to unmarshal doc: %v", err)
		}
	}
}

func TestBigArray(t *testing.T) {
	encoder := NewEncoder(4, 0)
	arrayGeneratorBig := &ArrayGenerator{
		EmptyGenerator: NewEmptyGenerator("key", 0, bson.ElementArray, encoder),
		Size:           15,
		Generator: &BoolGenerator{
			EmptyGenerator: NewEmptyGenerator("", 0, bson.ElementBool, encoder),
		},
	}

	generator := &ObjectGenerator{
		EmptyGenerator: NewEmptyGenerator("", 0, bson.ElementDocument, encoder),
		Generators:     []Generator{arrayGeneratorBig},
	}

	var a struct {
		Key []bool `bson:"key"`
	}
	for i := 0; i < 100; i++ {
		generator.Value()
		err := bson.Unmarshal(generator.Out.Data, &a)
		if err != nil {
			t.Error(err)
		}
		if len(a.Key) != arrayGeneratorBig.Size {
			t.Errorf("wrong array size, expected %d, got %d", arrayGeneratorBig.Size, len(a.Key))
		}
	}
}

func TestGetLength(t *testing.T) {
	emptyGenerator := NewEmptyGenerator("", 0, bson.ElementDocument, NewEncoder(4, 0))
	l := emptyGenerator.getLength(5, 5)
	if l != uint32(5) {
		t.Errorf("got wrong length, expected %d, got %d", uint32(5), l)
	}
	l = emptyGenerator.getLength(5, 10)
	if l > 10 || l < 5 {
		t.Errorf("length should be >= 5 and <= 10, but was %d", l)
	}
}

func TestGetUniqueArray(t *testing.T) {
	u := &uniqueGenerator{
		CurrentIndex: 0,
	}
	err := u.getUniqueArray(1000, 1)
	if err == nil {
		t.Error("getUniqueArray should fail because stringsize to low")
	}
	err = u.getUniqueArray(1000, 3)
	if err != nil {
		t.Error(err.Error())
	}
	if len(u.Values) != 1000 {
		t.Errorf("expected %d values but got %d", 1000, len(u.Values))
	}
}

func TestDocumentWithDecimal128(t *testing.T) {
	encoder := NewEncoder(4, 0)
	generator := &ObjectGenerator{
		EmptyGenerator: NewEmptyGenerator("", 0, bson.ElementDocument, encoder),
		Generators: []Generator{
			&Decimal128Generator{
				EmptyGenerator: NewEmptyGenerator("key", 0, bson.ElementDecimal128, encoder),
			},
		},
	}

	var d struct {
		Decimal bson.Decimal128 `bson:"decimal"`
	}
	for i := 0; i < 1000; i++ {
		generator.Value()
		err := bson.Unmarshal(generator.Out.Data, &d)
		if err != nil {
			t.Error(err)
		}
	}
}

func TestNewGeneratorCond(t *testing.T) {
	type testCase struct {
		name    string
		config  config.GeneratorJSON
		correct bool
		version []int
	}

	newGeneratorTests := []testCase{
		{
			name: "string ivalid minLength",
			config: config.GeneratorJSON{
				Type:      "string",
				MinLength: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "string invalid maxLength",
			config: config.GeneratorJSON{
				Type:      "string",
				MinLength: 5,
				MaxLength: 2,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "unique with length == 0",
			config: config.GeneratorJSON{
				Type:      "string",
				MinLength: 0,
				Unique:    true,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "maxDistinctValue too high",
			config: config.GeneratorJSON{
				Type:             "string",
				MinLength:        0,
				MaxDistinctValue: 10,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "int with invalid minInt32",
			config: config.GeneratorJSON{
				Type:     "int",
				MinInt32: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "int with invalid maxInt32",
			config: config.GeneratorJSON{
				Type:     "int",
				MinInt32: 10,
				MaxInt32: 4,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "long with invalid minInt64",
			config: config.GeneratorJSON{
				Type:     "long",
				MinInt64: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "long with invalid MaxInt64",
			config: config.GeneratorJSON{
				Type:     "long",
				MinInt64: 10,
				MaxInt64: 4,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "double with invalid minFloat",
			config: config.GeneratorJSON{
				Type:       "double",
				MinFloat64: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "double with invalid MaxFloat",
			config: config.GeneratorJSON{
				Type:       "double",
				MinFloat64: 10,
				MaxFloat64: 4,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "array with size < 0 ",
			config: config.GeneratorJSON{
				Type: "array",
				Size: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "array with invalid content",
			config: config.GeneratorJSON{
				Type: "array",
				Size: 3,
				ArrayContent: &config.GeneratorJSON{
					Type:      "string",
					MinLength: -1,
				},
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "empty fromArray",
			config: config.GeneratorJSON{
				Type: "fromArray",
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "fromArray with invalid BSON values",
			config: config.GeneratorJSON{
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
			config: config.GeneratorJSON{
				Type:      "binary",
				MinLength: -1,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "binary with incorrect MaxLength",
			config: config.GeneratorJSON{
				Type:      "binary",
				MinLength: 5,
				MaxLength: 2,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "date with incorrect bounds",
			config: config.GeneratorJSON{
				Type:      "date",
				StartDate: time.Now(),
				EndDate:   time.Unix(10, 10),
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "constant with invalid BSON value",
			config: config.GeneratorJSON{
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
			config: config.GeneratorJSON{
				Type:     "autoincrement",
				AutoType: "",
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "ref generator with invalid generator",
			config: config.GeneratorJSON{
				Type: "ref",
				RefContent: &config.GeneratorJSON{
					Type:      "string",
					MinLength: -1,
				},
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "object generator with invalid generator",
			config: config.GeneratorJSON{
				Type: "object",
				ObjectContent: map[string]config.GeneratorJSON{
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
			config:  config.GeneratorJSON{},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "null percentage > 100",
			config: config.GeneratorJSON{
				Type:           "string",
				NullPercentage: 120,
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "unknown faker val",
			config: config.GeneratorJSON{
				Type:   "faker",
				Method: "unknown",
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "type aggregator",
			config: config.GeneratorJSON{
				Type:           "countAggregator",
				NullPercentage: 10,
			},
			correct: true,
			version: []int{3, 6},
		},
		{
			name: "unknown type",
			config: config.GeneratorJSON{
				Type: "unknown",
			},
			correct: false,
			version: []int{3, 6},
		},
		{
			name: "decimal with mongodb 3.2",
			config: config.GeneratorJSON{
				Type: "decimal",
			},
			correct: false,
			version: []int{3, 2},
		},
		{
			name: "decimal with mongodb 3.6",
			config: config.GeneratorJSON{
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
			config: config.GeneratorJSON{
				Type:   "faker",
				Method: f,
			},
			correct: true,
			version: []int{3, 6},
		})
	}

	ci := NewCollInfo(1, false, nil, defaultSeed)

	for _, tt := range newGeneratorTests {
		t.Run(tt.name, func(t *testing.T) {
			ci.Version = tt.version
			_, err := ci.newGenerator("k", &tt.config)
			if tt.correct && err != nil {
				t.Errorf("expected no error for generator with config %v: \n%v", tt.config, err)
			}
			if !tt.correct && err == nil {
				t.Errorf("expected no error for generator with config %v", tt.config)
			}
		})
	}
}

func TestNewGeneratorFromMap(t *testing.T) {
	ci := NewCollInfo(1, true, []int{3, 4}, defaultSeed)

	m := map[string]config.GeneratorJSON{
		"key": {
			Type:      "string",
			MinLength: -1,
		},
	}
	_, err := ci.newGeneratorsFromMap(m)
	if err == nil {
		t.Error("newGeneratorFromMap should fail when map contains invalid generators")
	}
	_, err = ci.DocumentGenerator(m)
	if err == nil {
		t.Error("DocumentGenerator should fail when map contains invalid generators")
	}
}

func TestNewAggregatorCond(t *testing.T) {
	newAggregatorTests := []struct {
		name    string
		config  config.GeneratorJSON
		correct bool
	}{
		{
			name: "empty collection",
			config: config.GeneratorJSON{
				Type:       "countAggregator",
				Query:      bson.M{"n": 1},
				Database:   "db",
				Collection: "",
			},
			correct: false,
		},
		{
			name: "empty field valueAggregator",
			config: config.GeneratorJSON{
				Type:       "valueAggregator",
				Collection: "coll",
				Query:      bson.M{"n": 1},
				Database:   "db",
				Field:      "",
			},
			correct: false,
		},
		{
			name: "empty field boundAggregator",
			config: config.GeneratorJSON{
				Type:       "boundAggregator",
				Collection: "coll",
				Query:      bson.M{"n": 1},
				Database:   "db",
				Field:      "",
			},
			correct: false,
		},
		{
			name: "missing all",
			config: config.GeneratorJSON{
				Type: "countAggregator",
			},
			correct: false,
		},
		{
			name: "unknown aggregator type",
			config: config.GeneratorJSON{
				Type:       "unknown",
				Collection: "test",
				Database:   "test",
				Query:      bson.M{"n": 1},
			},
			correct: true,
		},
		{
			name: "empty query",
			config: config.GeneratorJSON{
				Type:  "countAggregator",
				Query: bson.M{},
			},
			correct: false,
		},
	}
	ci := NewCollInfo(1, false, []int{3, 4}, defaultSeed)

	for _, tt := range newAggregatorTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ci.newAggregator("k", &tt.config)
			if tt.correct && err != nil {
				t.Errorf("expected no error for config %v: \n%v", tt.config, err)
			} else if !tt.correct && err == nil {
				t.Errorf("expected an error for config %v but got none", tt.config)
			}
		})
	}
}

func TestNewAggregatorFromMap(t *testing.T) {
	content, err := ioutil.ReadFile("../samples/agg.json")
	if err != nil {
		t.Error(err)
	}
	aggColl, err := config.ParseConfig(content, false)
	if err != nil {
		t.Error(err)
	}

	newAggregatorFromMapTests := []struct {
		config       map[string]config.GeneratorJSON
		correct      bool
		aggregatorNb int
	}{
		{
			map[string]config.GeneratorJSON{
				"key": {
					Type:       "valueAggregator",
					Collection: "",
				},
			},
			false,
			0,
		}, {
			aggColl[0].Content,
			true,
			0,
		}, {
			aggColl[1].Content,
			true,
			3,
		},
	}

	ci := NewCollInfo(1, false, []int{3, 4}, defaultSeed)

	for _, tt := range newAggregatorFromMapTests {
		aggs, err := ci.DocumentAggregator(tt.config)
		if tt.correct && err != nil {
			t.Errorf("expected no error for config %v: \n%v", tt.config, err)
		} else if !tt.correct && err == nil {
			t.Errorf("expected an error for config %v but got none", tt.config)
		}
		if len(aggs) != tt.aggregatorNb {
			t.Errorf("for config %v, expected %d agg but got %d", tt.config, tt.aggregatorNb, len(aggs))
		}
	}
}

func TestClearMap(t *testing.T) {
	l := len(mapRef)
	if l > 0 {
		ClearRef()
	}
	if len(mapRef) != 0 {
		t.Errorf("wrong mapRef length, expected 0, got %d", len(mapRef))
	}
	if len(mapRefType) != 0 {
		t.Errorf("wrong mapRefType length, expected 0, got %d", len(mapRefType))
	}
}

func TestVersionAtLeast(t *testing.T) {
	versionTests := []struct {
		actualVersion, atLeastVersion []int
		response                      bool
	}{
		{[]int{2, 6}, []int{3, 4}, false},
		{[]int{3, 4}, []int{3, 2}, true},
		{[]int{3, 4}, []int{3, 4}, true},
		{[]int{}, []int{3, 4}, false},
	}

	for _, tt := range versionTests {
		ci := NewCollInfo(1, false, tt.actualVersion, defaultSeed)
		r := ci.versionAtLeast(tt.atLeastVersion...)
		if r != tt.response {
			t.Errorf("got %v, expected %v for test %v", r, tt.response, tt.actualVersion)
		}
	}
}

func BenchmarkGeneratorString(b *testing.B) {
	stringGenerator := &StringGenerator{
		EmptyGenerator: NewEmptyGenerator("key", 0, bson.ElementString, NewEncoder(4, 0)),
		MinLength:      5,
		MaxLength:      8,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stringGenerator.Value()
		stringGenerator.Out.Truncate(0)
	}
}
func BenchmarkGeneratorInt32(b *testing.B) {
	int32Generator := &Int32Generator{
		EmptyGenerator: NewEmptyGenerator("key", 0, bson.ElementInt32, NewEncoder(4, 0)),
		Min:            0,
		Max:            100,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		int32Generator.Value()
		int32Generator.Out.Truncate(0)
	}
}
func BenchmarkGeneratorInt64(b *testing.B) {
	int64Generator := &Int64Generator{
		EmptyGenerator: NewEmptyGenerator("key", 0, bson.ElementInt64, NewEncoder(4, 0)),
		Min:            0,
		Max:            100,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		int64Generator.Value()
		int64Generator.Out.Truncate(0)
	}
}
func BenchmarkGeneratorFloat64(b *testing.B) {
	float64Generator := &Float64Generator{
		EmptyGenerator: NewEmptyGenerator("key", 0, bson.ElementFloat64, NewEncoder(4, 0)),
		Mean:           0,
		StdDev:         50,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		float64Generator.Value()
		float64Generator.Out.Truncate(0)
	}
}
func BenchmarkGeneratorBool(b *testing.B) {
	boolGenerator := &BoolGenerator{
		EmptyGenerator: NewEmptyGenerator("key", 0, bson.ElementBool, NewEncoder(4, 0)),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		boolGenerator.Value()
		boolGenerator.Out.Truncate(0)
	}
}

func BenchmarkGeneratorPos(b *testing.B) {
	posGenerator := &PositionGenerator{
		EmptyGenerator: NewEmptyGenerator("key", 0, bson.ElementArray, NewEncoder(4, 0)),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		posGenerator.Value()
		posGenerator.Out.Truncate(0)
	}
}

func BenchmarkGeneratorObjectId(b *testing.B) {
	objectIDGenerator := &ObjectIDGenerator{
		EmptyGenerator: NewEmptyGenerator("_id", 0, bson.ElementObjectId, NewEncoder(4, 0)),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		objectIDGenerator.Value()
		objectIDGenerator.Out.Truncate(0)
	}
}

func BenchmarkGeneratorBinary(b *testing.B) {
	binaryGenerator := &BinaryDataGenerator{
		EmptyGenerator: NewEmptyGenerator("key", 0, bson.ElementBinary, NewEncoder(4, 0)),
		MinLength:      20,
		MaxLength:      40,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binaryGenerator.Value()
		binaryGenerator.Out.Truncate(0)
	}
}
func BenchmarkGeneratorDecimal128(b *testing.B) {
	decimal128Generator := &Decimal128Generator{
		EmptyGenerator: NewEmptyGenerator("key", 0, bson.ElementDecimal128, NewEncoder(4, 0)),
	}
	for i := 0; i < b.N; i++ {
		decimal128Generator.Value()
		decimal128Generator.Out.Truncate(0)
	}
}
func BenchmarkGeneratorDate(b *testing.B) {
	dateGenerator := &DateGenerator{
		EmptyGenerator: NewEmptyGenerator("key", 0, bson.ElementDatetime, NewEncoder(4, 0)),
		StartDate:      uint64(time.Now().Unix()),
		Delta:          200000,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dateGenerator.Value()
		dateGenerator.Out.Truncate(0)
	}
}

func BenchmarkGeneratorArray(b *testing.B) {
	encoder := NewEncoder(4, 0)
	arrayGenerator := &ArrayGenerator{
		EmptyGenerator: NewEmptyGenerator("key", 0, bson.ElementArray, encoder),
		Size:           5,
		Generator: &BoolGenerator{
			EmptyGenerator: NewEmptyGenerator("key", 0, bson.ElementBool, encoder),
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arrayGenerator.Value()
		arrayGenerator.Out.Truncate(0)
	}
}

func BenchmarkGeneratorAll(b *testing.B) {
	content, err := ioutil.ReadFile("../samples/config.json")
	if err != nil {
		b.Fail()
	}
	collectionList, err := config.ParseConfig(content, false)

	ci := NewCollInfo(1000, false, []int{3, 2}, defaultSeed)

	generator, _ := ci.DocumentGenerator(collectionList[0].Content)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generator.Value()
	}
}
