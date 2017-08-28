package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/globalsign/mgo/bson"

	rg "github.com/feliixx/mgodatagen/generators"
)

var (
	source               = rg.NewRandSource()
	sd, _                = time.Parse(time.RFC3339, "2010-01-01T00:00:00-00:00")
	ed, _                = time.Parse(time.RFC3339, "2016-01-01T00:00:00-00:00")
	eg                   = rg.EmptyGenerator{K: "key", NullPercentage: 100, T: 8}
	constArr             = []interface{}{"2012-10-10", "2012-12-12", "2014-01-01", "2016-05-05"}
	stringGenerator      = &rg.StringGenerator{EmptyGenerator: rg.EmptyGenerator{K: "key", NullPercentage: 100, T: 0}, MinLength: 2, MaxLength: 5}
	int32Generator       = &rg.Int32Generator{EmptyGenerator: rg.EmptyGenerator{K: "key", NullPercentage: 100, T: 2}, Min: 0, Max: 100}
	int64Generator       = &rg.Int64Generator{EmptyGenerator: rg.EmptyGenerator{K: "key", NullPercentage: 100, T: 3}, Min: 0, Max: 100}
	float64Generator     = &rg.Float64Generator{EmptyGenerator: rg.EmptyGenerator{K: "key", NullPercentage: 100, T: 4}, Mean: 0, StdDev: 50}
	dateGenerator        = &rg.DateGenerator{EmptyGenerator: rg.EmptyGenerator{K: "key", NullPercentage: 100, T: 7}, StartDate: sd.Unix(), Delta: (ed.Unix() - sd.Unix())}
	boolGenerator        = &rg.BoolGenerator{EmptyGenerator: rg.EmptyGenerator{K: "key", NullPercentage: 100, T: 1}}
	binaryGenerator      = &rg.BinaryDataGenerator{EmptyGenerator: eg, MinLength: 30, MaxLength: 30}
	positionGenerator    = &rg.PositionGenerator{EmptyGenerator: eg}
	arrayGeneratorString = &rg.ArrayGenerator{EmptyGenerator: eg, Size: 10, Generator: stringGenerator}
	arrayGeneratorBool   = &rg.ArrayGenerator{EmptyGenerator: eg, Size: 10, Generator: boolGenerator}
	autoIncrGenerator    = &rg.AutoIncrementGenerator{EmptyGenerator: rg.EmptyGenerator{K: "key", NullPercentage: 100, T: 3}, Counter: 0}
	objectIDGenerator    = &rg.ObjectIDGenerator{EmptyGenerator: rg.EmptyGenerator{K: "key", NullPercentage: 100, T: 5}}
	refGenerator         = &rg.RefGenerator{EmptyGenerator: eg, ID: 1, Generator: objectIDGenerator}
	fromArrayGenerator   = &rg.FromArrayGenerator{EmptyGenerator: eg, Array: constArr, Size: int32(len(constArr)), Index: -1}
)

type expected struct {
	ID          bson.ObjectId `json:"_id"`
	Date        time.Time     `json:"date"`
	Data        []byte        `json:"data"`
	Dt          string        `json:"dt"`
	Name        string        `json:"name"`
	Count32     int32         `json:"count32"`
	Count64     int64         `json:"count64"`
	Float       float64       `json:"float"`
	Verified    bool          `json:"verified"`
	FirstArray  [3]string     `json:"firstArray"`
	Nb          int           `json:"nb"`
	Pos         [2]float32    `json:"pos"`
	Cs          cst           `json:"cst"`
	FirstObject obj           `json:"firstObject"`
}
type obj struct {
	K  int    `json:"k"`
	Nm string `json:"nm"`
}
type cst struct {
	Key1 string `json:"key1"`
	Key2 int32  `json:"key2"`
}

func getGeneratorFromFile(shortNames bool) (rg.Generator, error) {
	file, err := ioutil.ReadFile("samples/config.json")
	if err != nil {
		return nil, fmt.Errorf("File error: %v", err.Error())
	}
	var collectionList []Collection
	err = json.Unmarshal(file, &collectionList)
	if err != nil {
		return nil, fmt.Errorf("JSON error: %v", err.Error())
	}
	generators, err := rg.NewGeneratorsFromMap(collectionList[0].Content, shortNames)
	if err != nil {
		return nil, fmt.Errorf("Config error: %v", err.Error())
	}
	eg := rg.EmptyGenerator{K: "", NullPercentage: 0, T: 6}
	return &rg.ArrayGenerator{EmptyGenerator: eg,
		Size:      1000,
		Generator: &rg.ObjectGenerator{EmptyGenerator: eg, Generators: generators}}, nil
}

func BenchmarkExists(b *testing.B) {
	for n := 0; n < b.N; n++ {
		stringGenerator.Exists(source)
	}
}

func BenchmarkRandomString(b *testing.B) {
	for n := 0; n < b.N; n++ {
		stringGenerator.Value(source)
	}
}

func BenchmarkRandomInt32(b *testing.B) {
	for n := 0; n < b.N; n++ {
		int32Generator.Value(source)
	}
}
func BenchmarkRandomInt64(b *testing.B) {
	for n := 0; n < b.N; n++ {
		int64Generator.Value(source)
	}
}
func BenchmarkRandomFloat64(b *testing.B) {
	for n := 0; n < b.N; n++ {
		float64Generator.Value(source)
	}
}
func BenchmarkRandomDate(b *testing.B) {
	for n := 0; n < b.N; n++ {
		dateGenerator.Value(source)
	}
}

func BenchmarkRandomBool(b *testing.B) {
	for n := 0; n < b.N; n++ {
		boolGenerator.Value(source)
	}
}

func BenchmarkRandomBinaryData(b *testing.B) {
	for n := 0; n < b.N; n++ {
		binaryGenerator.Value(source)
	}
}
func BenchmarkRandomPosition(b *testing.B) {
	for n := 0; n < b.N; n++ {
		positionGenerator.Value(source)
	}
}
func BenchmarkRandomArray(b *testing.B) {
	var list []string
	for n := 0; n < b.N; n++ {
		list = arrayGeneratorString.Value(source).([]string)
	}
	_ = list[0]

}
func BenchmarkRandomArray1(b *testing.B) {
	var list []bool
	for n := 0; n < b.N; n++ {
		list = arrayGeneratorBool.Value(source).([]bool)
	}
	_ = list[0]
}
func BenchmarkAutoIncrement(b *testing.B) {
	for n := 0; n < b.N; n++ {
		autoIncrGenerator.Value(source)
	}
}
func BenchmarkRefGenerator(b *testing.B) {
	for n := 0; n < b.N; n++ {
		refGenerator.Value(source)
	}
}

func BenchmarkFromArray(b *testing.B) {
	for n := 0; n < b.N; n++ {
		fromArrayGenerator.Value(source)
	}
}

func BenchmarkJSONGeneration(b *testing.B) {
	generator, err := getGeneratorFromFile(false)
	if err != nil {
		b.Error(err.Error())
	}
	docList := generator.Value(source).([]bson.M)
	bsonBytes, err := bson.Marshal(docList[0])
	if err != nil {
		fmt.Printf("marshaling failed, %s\n", err.Error())
	}
	// on each generation, 1000 docs are generated
	b.SetBytes(int64(len(bsonBytes)) * 1000)
	for n := 0; n < b.N; n++ {
		docList = generator.Value(source).([]bson.M)
	}
	_ = docList[0]
}

func BenchmarkJSONGenerationShortNames(b *testing.B) {
	generator, err := getGeneratorFromFile(true)
	if err != nil {
		b.Error(err.Error())
	}
	docList := generator.Value(source).([]bson.M)
	bsonBytes, err := bson.Marshal(docList[0])
	if err != nil {
		fmt.Printf("marshaling failed, %s\n", err.Error())
	}
	// on each generation, 1000 docs are generated
	b.SetBytes(int64(len(bsonBytes)) * 1000)
	for n := 0; n < b.N; n++ {
		docList = generator.Value(source).([]bson.M)
	}
	_ = docList[0]
}

func TestKey(t *testing.T) {
	if stringGenerator.Key() != "key" {
		t.Error("wrong key returned")
	}
}
func TestExists(t *testing.T) {
	tc := 0
	fc := 0
	for i := 0; i < 1000; i++ {
		if stringGenerator.Exists(source) {
			tc++
		} else {
			fc++
		}
	}
	if fc > 150 || fc < 50 {
		t.Errorf("Error to big, should accept 5 percent, but got  %v percent", fc/10)
	}

}
func TestValueString(t *testing.T) {
	for i := 0; i < 1000; i++ {
		v := stringGenerator.Value(source).(string)
		if int32(len(v)) > stringGenerator.MaxLength || int32(len(v)) < stringGenerator.MinLength {
			t.Error("wrong string size")
		}
	}
}

func TestValueBool(t *testing.T) {
	v := boolGenerator.Value(source).(bool)
	if v != true && v != false {
		t.Error("wrong value for bool generator")
	}
}

func TestInt32Value(t *testing.T) {
	for i := 0; i < 1000; i++ {
		v := int32Generator.Value(source).(int32)
		if v > int32Generator.Max || v < int32Generator.Min {
			t.Error("int32 not within correct bounds")
		}
	}
}

func TestInt64Value(t *testing.T) {
	for i := 0; i < 1000; i++ {
		v := int64Generator.Value(source).(int64)
		if v > int64Generator.Max || v < int64Generator.Min {
			t.Error("int64 not within correct bounds")
		}
	}
}
func TestFloat64Value(t *testing.T) {
	for i := 0; i < 1000; i++ {
		v := float64Generator.Value(source).(float64)
		if v > (float64Generator.Mean+float64Generator.StdDev) || v < (float64Generator.Mean-float64Generator.StdDev) {
			t.Error("float64 not within correct bounds")
		}
	}
}
func TestDateValue(t *testing.T) {
	for i := 0; i < 1000; i++ {
		v := dateGenerator.Value(source).(time.Time)
		if v.After(ed) || v.Before(sd) {
			t.Error("Date not within correct bounds")
		}
	}
}
func TestBinaryDataValue(t *testing.T) {
	for i := 0; i < 1000; i++ {
		v := binaryGenerator.Value(source).([]byte)
		if int32(len(v)) > binaryGenerator.MaxLength || int32(len(v)) < binaryGenerator.MinLength {
			t.Error("wrong string size")
		}
	}
}

func TestPositionValue(t *testing.T) {
	for i := 0; i < 1000; i++ {
		v := positionGenerator.Value(source).([2]float32)
		if v[0] < -90 || v[0] > 90 {
			t.Errorf("wrong value for longitude: %v", v[0])
		}
		if v[1] < -180 || v[1] > 180 {
			t.Errorf("wrong value for latitiude: %v", v[1])
		}
	}
}
func TestStringArrayValue(t *testing.T) {
	v := arrayGeneratorString.Value(source).([]string)
	if len(v) != arrayGeneratorString.Size {
		t.Errorf("wrong aray size, expected %v, got %v", arrayGeneratorString.Size, len(v))
	}
}

func TestAutoIncrementValue(t *testing.T) {
	var v int64
	for i := 0; i < 100; i++ {
		v = autoIncrGenerator.Value(source).(int64)
	}
	if v != 100 {
		t.Errorf("wrong value for autoincrement field, expected 100, got %v", v)
	}
}
func TestFromArrayValue(t *testing.T) {
	var v string
	for i := 0; i < 100; i++ {
		v = fromArrayGenerator.Value(source).(string)
		ok := false
		for j := range constArr {
			if constArr[j] == v {
				ok = true
			}
		}
		if !ok {
			t.Errorf("wrong value for fromArray field, %s is not in %v", v, constArr)
		}
	}
}
func TestGeneratedDoc(t *testing.T) {
	var exp expected
	generator, err := getGeneratorFromFile(false)
	if err != nil {
		t.Error(err.Error())
	}
	docList := generator.Value(source).([]bson.M)
	for _, doc := range docList {
		bsonBytes, err := bson.Marshal(doc)
		if err != nil {
			t.Errorf("Couldn't marshal generated documenT: %s", err.Error())
		}
		err = bson.Unmarshal(bsonBytes, &exp)
		if err != nil {
			t.Errorf("Couldn't unmarshal generated documenT: %s", err.Error())
		}
	}
}
