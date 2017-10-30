package generators

import (
	"math/rand"
	"testing"
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/stretchr/testify/assert"

	"github.com/feliixx/mgodatagen/config"
)

var (
	rndSrc            = rand.NewSource(time.Now().UnixNano())
	encoder           = &Encoder{Data: make([]byte, 4), R: rand.New(rndSrc), Src: rndSrc}
	stringGenerator   = &StringGenerator{EmptyGenerator: EmptyGenerator{K: append([]byte("key1"), byte(0)), NullPercentage: 100, T: bson.ElementString, Out: encoder}, MinLength: 5, MaxLength: 5}
	int32Generator    = &Int32Generator{EmptyGenerator: EmptyGenerator{K: append([]byte("key2"), byte(0)), NullPercentage: 100, T: bson.ElementInt32, Out: encoder}, Min: 0, Max: 100}
	int64Generator    = &Int64Generator{EmptyGenerator: EmptyGenerator{K: append([]byte("key2"), byte(0)), NullPercentage: 0, T: bson.ElementInt64, Out: encoder}, Min: 0, Max: 100}
	float64Generator  = &Float64Generator{EmptyGenerator: EmptyGenerator{K: append([]byte("key4"), byte(0)), NullPercentage: 100, T: bson.ElementFloat64, Out: encoder}, Mean: 0, StdDev: 50}
	boolGenerator     = &BoolGenerator{EmptyGenerator: EmptyGenerator{K: append([]byte("key5"), byte(0)), NullPercentage: 100, T: bson.ElementBool, Out: encoder}}
	posGenerator      = &PositionGenerator{EmptyGenerator: EmptyGenerator{K: append([]byte("key6"), byte(0)), NullPercentage: 0, T: bson.ElementArray, Out: encoder}}
	objectIDGenerator = &ObjectIDGenerator{EmptyGenerator: EmptyGenerator{K: append([]byte("_id"), byte(0)), NullPercentage: 0, T: bson.ElementObjectId, Out: encoder}}
)

type expectedDoc struct {
	ID         bson.ObjectId   `bson:"_id"`
	Name       string          `bson:"name"`
	C32        int32           `bson:"c32"`
	C64        int64           `bson:"c64"`
	Float      float64         `bson:"float"`
	Dec        bson.Decimal128 `bson:"dec"`
	Verified   bool            `bson:"verified"`
	Position   []float64       `bson:"position"`
	Dt         string          `bson:"dt"`
	Fake       string          `bson:"faker"`
	Cst        int32           `bson:"cst"`
	Nb         int64           `bson:"nb"`
	Date       time.Time       `bson:"date"`
	BinaryData []byte          `bson:"binaryData"`
	List       []int32         `bson:"list"`
	Object     struct {
		K1    string `bson:"k1"`
		K2    int32  `bson:"k2"`
		Subob struct {
			Sk int32 `bson:"s-k"`
		} `bson:"sub-ob"`
	} `bson:"object"`
}

func TestIsDocumentCorrect(t *testing.T) {

	collectionList, err := config.CollectionList("../samples/bson_test.json")
	assert.Nil(t, err)

	src := rand.NewSource(time.Now().UnixNano())

	encoder := &Encoder{
		Data: make([]byte, 4),
		R:    rand.New(src),
		Src:  src,
	}
	generator, err := CreateGenerator(collectionList[0].Content, false, 1000, encoder)
	assert.Nil(t, err)

	var d expectedDoc

	for i := 0; i < 1000; i++ {
		generator.Value()
		err := bson.Unmarshal(encoder.Data, &d)
		assert.Nil(t, err)
	}

}

func BenchmarkGeneratorString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stringGenerator.Value()
	}
}
func BenchmarkGeneratorInt32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		int32Generator.Value()
	}
}
func BenchmarkGeneratorInt64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		int64Generator.Value()
	}
}
func BenchmarkGeneratorFloat64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		float64Generator.Value()
	}
}
func BenchmarkGeneratorBool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		boolGenerator.Value()
	}
}

func BenchmarkGeneratorPos(b *testing.B) {
	for i := 0; i < b.N; i++ {
		posGenerator.Value()
	}
}

func BenchmarkGeneratorObjectId(b *testing.B) {
	for i := 0; i < b.N; i++ {
		objectIDGenerator.Value()
	}
}

func BenchmarkGeneratorAll(b *testing.B) {
	b.StopTimer()
	collectionList, _ := config.CollectionList("../samples/config.json")

	src := rand.NewSource(time.Now().UnixNano())

	encoder := &Encoder{
		Data: make([]byte, 4),
		R:    rand.New(src),
		Src:  src,
	}
	generator, _ := CreateGenerator(collectionList[0].Content, false, 1000, encoder)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		generator.Value()
	}
}
