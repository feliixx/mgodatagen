package generators

import (
	"testing"
	"time"

	"github.com/MichaelTJones/pcg"
	"github.com/globalsign/mgo/bson"
	"github.com/stretchr/testify/require"

	"github.com/feliixx/mgodatagen/config"
)

var (
	encoder = &Encoder{
		Data:  make([]byte, 4),
		PCG32: pcg.NewPCG32().Seed(1, 1),
		PCG64: pcg.NewPCG64().Seed(1, 1, 1, 1),
	}
	stringGenerator = &StringGenerator{
		EmptyGenerator: EmptyGenerator{
			K:              append([]byte("key1"), byte(0)),
			NullPercentage: 100,
			T:              bson.ElementString,
			Out:            encoder,
		},
		MinLength: 5,
		MaxLength: 8,
	}
	int32Generator = &Int32Generator{
		EmptyGenerator: EmptyGenerator{
			K:              append([]byte("key2"), byte(0)),
			NullPercentage: 100,
			T:              bson.ElementInt32,
			Out:            encoder,
		},
		Min: 0,
		Max: 100,
	}
	int64Generator = &Int64Generator{
		EmptyGenerator: EmptyGenerator{
			K:              append([]byte("key2"), byte(0)),
			NullPercentage: 0,
			T:              bson.ElementInt64,
			Out:            encoder,
		},
		Min: 0,
		Max: 100,
	}
	float64Generator = &Float64Generator{
		EmptyGenerator: EmptyGenerator{
			K:              append([]byte("key4"), byte(0)),
			NullPercentage: 100,
			T:              bson.ElementFloat64,
			Out:            encoder,
		},
		Mean:   0,
		StdDev: 50,
	}
	boolGenerator = &BoolGenerator{
		EmptyGenerator: EmptyGenerator{
			K:              append([]byte("key5"), byte(0)),
			NullPercentage: 100,
			T:              bson.ElementBool,
			Out:            encoder,
		},
	}
	posGenerator = &PositionGenerator{
		EmptyGenerator: EmptyGenerator{
			K:              append([]byte("key6"), byte(0)),
			NullPercentage: 0,
			T:              bson.ElementArray,
			Out:            encoder,
		},
	}
	objectIDGenerator = &ObjectIDGenerator{
		EmptyGenerator: EmptyGenerator{
			K:              append([]byte("_id"), byte(0)),
			NullPercentage: 0,
			T:              bson.ElementObjectId,
			Out:            encoder,
		},
	}
	binaryGenerator = &BinaryDataGenerator{
		EmptyGenerator: EmptyGenerator{
			K:              append([]byte("key0"), byte(0)),
			NullPercentage: 0,
			T:              bson.ElementBinary,
			Out:            encoder,
		},
		MinLength: 20,
		MaxLength: 40,
	}
	dateGenerator = &DateGenerator{
		EmptyGenerator: EmptyGenerator{
			K:              append([]byte("key7"), byte(0)),
			NullPercentage: 0,
			T:              bson.ElementDatetime,
			Out:            encoder,
		},
		StartDate: uint64(time.Now().Unix()),
		Delta:     200000,
	}
	decimal128Generator = &Decimal128Generator{
		EmptyGenerator: EmptyGenerator{
			K:              append([]byte("key8"), byte(0)),
			NullPercentage: 0,
			T:              bson.ElementDecimal128,
			Out:            encoder,
		},
	}
	arrayGenerator = &ArrayGenerator{
		EmptyGenerator: EmptyGenerator{
			K:              append([]byte("key9"), byte(0)),
			NullPercentage: 0,
			T:              bson.ElementArray,
			Out:            encoder,
		},
		Size:      5,
		Generator: boolGenerator,
	}
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

type dec128Doc struct {
	Decimal bson.Decimal128 `bson:"decimal"`
}

func TestIsDocumentCorrect(t *testing.T) {
	assert := require.New(t)
	collectionList, err := config.CollectionList("../samples/bson_test.json")
	assert.Nil(err)
	now := time.Now().UnixNano()
	encoder := &Encoder{
		Data:  make([]byte, 4),
		PCG32: pcg.NewPCG32().Seed(uint64(now), uint64(now)),
		PCG64: pcg.NewPCG64().Seed(1, 1, 1, 1),
	}
	generator, err := CreateGenerator(collectionList[0].Content, false, 1000, []int{3, 2}, encoder)
	assert.Nil(err)

	var d expectedDoc

	for i := 0; i < 1000; i++ {
		generator.Value()
		err := bson.Unmarshal(encoder.Data, &d)
		assert.Nil(err)
	}
}

func TestDocumentWithDecimal128(t *testing.T) {
	assert := require.New(t)
	generator := &ObjectGenerator{
		EmptyGenerator: EmptyGenerator{K: []byte(""),
			NullPercentage: 0,
			T:              bson.ElementDocument,
			Out:            encoder,
		},
		Generators: []Generator{decimal128Generator},
	}

	var d dec128Doc
	for i := 0; i < 1000; i++ {
		generator.Value()
		err := bson.Unmarshal(encoder.Data, &d)
		assert.Nil(err)
	}
}

func TestVersionAtLeast(t *testing.T) {
	assert := require.New(t)
	assert.Equal(versionAtLeast([]int{2, 6}, 3, 4), false)
	assert.Equal(versionAtLeast([]int{3, 4}, 3, 2), true)
	assert.Equal(versionAtLeast([]int{3, 4}, 3, 4), true)
}

func BenchmarkGeneratorString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stringGenerator.Value()
		encoder.Data = encoder.Data[0:0]
	}
}
func BenchmarkGeneratorInt32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		int32Generator.Value()
		encoder.Data = encoder.Data[0:0]
	}
}
func BenchmarkGeneratorInt64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		int64Generator.Value()
		encoder.Data = encoder.Data[0:0]
	}
}
func BenchmarkGeneratorFloat64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		float64Generator.Value()
		encoder.Data = encoder.Data[0:0]
	}
}
func BenchmarkGeneratorBool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		boolGenerator.Value()
		encoder.Data = encoder.Data[0:0]
	}
}

func BenchmarkGeneratorPos(b *testing.B) {
	for i := 0; i < b.N; i++ {
		posGenerator.Value()
		encoder.Data = encoder.Data[0:0]
	}
}

func BenchmarkGeneratorObjectId(b *testing.B) {
	for i := 0; i < b.N; i++ {
		objectIDGenerator.Value()
		encoder.Data = encoder.Data[0:0]
	}
}

func BenchmarkGeneratorBinary(b *testing.B) {
	for i := 0; i < b.N; i++ {
		binaryGenerator.Value()
		encoder.Data = encoder.Data[0:0]
	}
}
func BenchmarkGeneratorDecimal128(b *testing.B) {
	for i := 0; i < b.N; i++ {
		decimal128Generator.Value()
		encoder.Data = encoder.Data[0:0]
	}
}
func BenchmarkGeneratorDate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		dateGenerator.Value()
		encoder.Data = encoder.Data[0:0]
	}
}

func BenchmarkGeneratorArray(b *testing.B) {
	for i := 0; i < b.N; i++ {
		arrayGenerator.Value()
		encoder.Data = encoder.Data[0:0]
	}
}

func BenchmarkGeneratorAll(b *testing.B) {
	collectionList, _ := config.CollectionList("../samples/config.json")

	encoder := &Encoder{
		Data:  make([]byte, 4),
		PCG32: pcg.NewPCG32().Seed(1, 1),
		PCG64: pcg.NewPCG64().Seed(1, 1, 1, 1),
	}
	generator, _ := CreateGenerator(collectionList[0].Content, false, 1000, []int{3, 2}, encoder)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generator.Value()
	}
}
