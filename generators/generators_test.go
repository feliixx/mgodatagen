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
	ID       bson.ObjectId `bson:"_id"`
	Name     string        `bson:"name"`
	C32      int32         `bson:"c32"`
	C64      int64         `bson:"c64"`
	Float    float64       `bson:"float"`
	Verified bool          `bson:"verified"`
	Position []float64     `bson:"position"`
	Dt       string        `bson:"dt"`
	Afa      []string      `bson:"afa"`
	//	Ac         []string      `bson:"ac"`
	Fake       string    `bson:"faker"`
	Cst        int32     `bson:"cst"`
	Nb         int64     `bson:"nb"`
	Nnb        int32     `bson:"nnb"`
	Date       time.Time `bson:"date"`
	BinaryData []byte    `bson:"binaryData"`
	List       []int32   `bson:"list"`
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

	e := NewEncoder(4)

	ci := &CollInfo{
		Encoder:    e,
		Version:    []int{3, 2},
		ShortNames: false,
		Count:      1000,
	}

	generator, err := ci.CreateGenerator(collectionList[0].Content)
	assert.Nil(err)

	var d expectedDoc

	for i := 0; i < 1000; i++ {
		generator.Value()
		err := bson.Unmarshal(ci.Encoder.Data, &d)
		assert.Nil(err)
	}
}

func TestBigArray(t *testing.T) {
	assert := require.New(t)
	arrayGenratorBig := &ArrayGenerator{
		EmptyGenerator: EmptyGenerator{
			K:              append([]byte("key"), byte(0)),
			NullPercentage: 0,
			T:              bson.ElementArray,
			Out:            encoder,
		},
		Size:      15,
		Generator: boolGenerator,
	}

	generator := &ObjectGenerator{
		EmptyGenerator: EmptyGenerator{K: []byte(""),
			NullPercentage: 0,
			T:              bson.ElementDocument,
			Out:            encoder,
		},
		Generators: []Generator{arrayGenratorBig},
	}

	var a struct {
		Key []bool `bson:"key"`
	}
	for i := 0; i < 100; i++ {
		generator.Value()
		err := bson.Unmarshal(encoder.Data, &a)
		assert.Nil(err)
		assert.Equal(arrayGenratorBig.Size, len(a.Key))
	}
}

func TestGetLength(t *testing.T) {
	assert := require.New(t)
	emptyGenerator := EmptyGenerator{K: []byte(""),
		NullPercentage: 0,
		T:              bson.ElementDocument,
		Out:            encoder,
	}

	l := emptyGenerator.getLength(5, 5)
	assert.Equal(uint32(5), l)

	l = emptyGenerator.getLength(5, 10)
	assert.True(l <= 10)
	assert.True(l >= 5)
}

func TestGetUniqueArray(t *testing.T) {
	assert := require.New(t)
	u := &UniqueGenerator{
		CurrentIndex: 0,
	}

	err := u.getUniqueArray(1000, 1)
	assert.NotNil(err)

	err = u.getUniqueArray(1000, 3)
	assert.Nil(err)
	assert.Equal(1000, len(u.Values))
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

func TestNewGenerator(t *testing.T) {
	assert := require.New(t)

	version := []int{3, 2}

	genJSON := &config.GeneratorJSON{
		NullPercentage: 120,
	}
	ci := &CollInfo{
		Encoder:    encoder,
		ShortNames: false,
		Count:      1,
		Version:    version,
	}

	_, err := ci.newGenerator("key", genJSON)
	assert.NotNil(err)

	ci.ShortNames = true

	genJSON.NullPercentage = 10
	genJSON.Type = "countAggregator"
	g, err := ci.newGenerator("key", genJSON)
	assert.Nil(g)
	assert.Nil(err)

	genJSON.Type = "unknown"
	_, err = ci.newGenerator("key", genJSON)
	assert.NotNil(err)

	genJSON.Type = "decimal"
	_, err = ci.newGenerator("key", genJSON)
	assert.NotNil(err)

	ci.Version = []int{3, 4}
	_, err = ci.newGenerator("key", genJSON)
	assert.Nil(err)

}

func TestNewAggregator(t *testing.T) {
	assert := require.New(t)

	genJSON := &config.GeneratorJSON{
		Type: "countAggregator",
	}

	ci := &CollInfo{
		ShortNames: true,
	}

	_, err := ci.newAggregator("key", genJSON)
	assert.NotNil(err)

	genJSON.Query = bson.M{"n": 1}
	_, err = ci.newAggregator("key", genJSON)
	assert.NotNil(err)

	genJSON.Database = "db"
	genJSON.Collection = "coll"

	_, err = ci.newAggregator("key", genJSON)
	assert.Nil(err)

	genJSON.Type = "unknown"
	_, err = ci.newAggregator("key", genJSON)
	assert.Nil(err)

	aggColl, err := config.CollectionList("../samples/agg.json")
	assert.Nil(err)

	ci.ShortNames = false

	aggs, err := ci.NewAggregatorFromMap(aggColl[0].Content)
	assert.Nil(err)
	assert.Equal(0, len(aggs))

	aggs, err = ci.NewAggregatorFromMap(aggColl[1].Content)
	assert.Nil(err)
	assert.Equal(3, len(aggs))
}

func TestVersionAtLeast(t *testing.T) {
	assert := require.New(t)

	ci := &CollInfo{
		Version: []int{2, 6},
	}
	assert.Equal(ci.versionAtLeast(3, 4), false)
	ci.Version = []int{3, 4}
	assert.Equal(ci.versionAtLeast(3, 2), true)
	ci.Version = []int{3, 4}
	assert.Equal(ci.versionAtLeast(3, 4), true)
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

	ci := &CollInfo{
		Encoder:    encoder,
		ShortNames: false,
		Count:      1000,
		Version:    []int{3, 2},
	}
	generator, _ := ci.CreateGenerator(collectionList[0].Content)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generator.Value()
	}
}
