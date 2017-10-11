package generatorsBSON

import (
	"testing"
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/stretchr/testify/assert"

	cf "github.com/feliixx/mgodatagen/config"
)

var (
	source               = NewRandSource()
	encoder              = &Encoder{Data: make([]byte, 4), DocCount: int32(0)}
	stringGenerator      = &StringGenerator{EmptyGenerator: EmptyGenerator{K: append([]byte("key1"), byte(0)), NullPercentage: 100, T: bson.ElementString, Out: encoder}, MinLength: 5, MaxLength: 5}
	int32Generator       = &Int32Generator{EmptyGenerator: EmptyGenerator{K: append([]byte("key2"), byte(0)), NullPercentage: 100, T: bson.ElementInt32, Out: encoder}, Min: 0, Max: 100}
	int64Generator       = &Int64Generator{EmptyGenerator: EmptyGenerator{K: append([]byte("key2"), byte(0)), NullPercentage: 0, T: bson.ElementInt64, Out: encoder}, Min: 0, Max: 100}
	float64Generator     = &Float64Generator{EmptyGenerator: EmptyGenerator{K: append([]byte("key4"), byte(0)), NullPercentage: 100, T: bson.ElementFloat64, Out: encoder}, Mean: 0, StdDev: 50}
	boolGenerator        = &BoolGenerator{EmptyGenerator: EmptyGenerator{K: append([]byte("key5"), byte(0)), NullPercentage: 100, T: bson.ElementBool, Out: encoder}}
	objectIDGenerator    = &ObjectIDGenerator{EmptyGenerator: EmptyGenerator{K: append([]byte("_id"), byte(0)), NullPercentage: 100, T: bson.ElementObjectId, Out: encoder}}
	gen                  = []Generator{stringGenerator, int32Generator, int64Generator, boolGenerator, float64Generator}
	objectGenerator      = &ObjectGenerator{EmptyGenerator: EmptyGenerator{K: []byte(""), NullPercentage: 100, T: bson.ElementDocument, Out: encoder}, Generators: gen}
	embededDocGenerator  = &EmbededObjectGenerator{EmptyGenerator: EmptyGenerator{K: append([]byte("key1"), byte(0)), NullPercentage: 0, T: bson.ElementDocument, Out: encoder}, Generators: []Generator{int64Generator}}
	embededDocGenerator2 = &EmbededObjectGenerator{EmptyGenerator: EmptyGenerator{K: append([]byte("key2"), byte(0)), NullPercentage: 0, T: bson.ElementDocument, Out: encoder}, Generators: []Generator{embededDocGenerator}}
	arrayGenerator       = &ArrayGenerator{EmptyGenerator: EmptyGenerator{K: append([]byte("key1"), byte(0)), NullPercentage: 0, T: bson.ElementArray, Out: encoder}, Generator: int32Generator, Size: int(3)}
)

type expectedDoc struct {
	ID         bson.ObjectId `bson:"_id"`
	Name       string        `bson:"name"`
	C32        int32         `bson:"c32"`
	C64        int64         `bson:"c64"`
	Float      float64       `bson:"float"`
	Verified   bool          `bson:"verified"`
	Position   []float64     `bson:"position"`
	Dt         string        `bson:"dt"`
	Cst        int32         `bson:"cst"`
	Nb         int64         `bson:"nb"`
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

	collectionList, err := cf.CollectionList("../samples/bson_test.json")
	assert.Nil(t, err)

	encoder := &Encoder{
		Data:     make([]byte, 4),
		DocCount: int32(0),
	}
	generator, err := CreateGenerator(collectionList[0].Content, false, 1000, encoder)
	assert.Nil(t, err)

	source := NewRandSource()
	var d expectedDoc

	for i := 0; i < 1000; i++ {
		generator.Value(source)
		err := bson.Unmarshal(encoder.Data, &d)
		assert.Nil(t, err)
	}

}

func BenchmarkGeneratorString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stringGenerator.Value(source)
	}
}
func BenchmarkGeneratorInt32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		int32Generator.Value(source)
	}
}
func BenchmarkGeneratorInt64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		int64Generator.Value(source)
	}
}
func BenchmarkGeneratorFloat64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		float64Generator.Value(source)
	}
}
func BenchmarkGeneratorBool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		boolGenerator.Value(source)
	}
}

func BenchmarkGeneratorObjectId(b *testing.B) {
	for i := 0; i < b.N; i++ {
		objectIDGenerator.Value(source)
	}
}

func BenchmarkGeneratorAll(b *testing.B) {
	for i := 0; i < b.N; i++ {
		objectGenerator.Value(source)
	}
}
