package generatorsBSON

import (
	"fmt"
	"testing"

	"github.com/globalsign/mgo/bson"
)

var (
	source               = NewRandSource()
	encoder              = &Encoder{Data: make([]byte, 4), DocCount: int(0)}
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

func TestGeneratorString(t *testing.T) {
	stringGenerator.Value(source)
	fmt.Printf("array: %v\n", stringGenerator.Out.Data)
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

// func BenchmarkGeneratorObjectId(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		objectIDGenerator.Value(source)
// 	}
// }

func BenchmarkGeneratorAll(b *testing.B) {
	for i := 0; i < b.N; i++ {
		objectGenerator.Value(source)
		// data := make([]byte, (encoder.DocSize + 5))
		// copy(data, encoder.Data)
	}
}

func TestFullGen(t *testing.T) {
	gen := []Generator{arrayGenerator}
	// , int32Generator, int64Generator, boolGenerator, float64Generator, objectIDGenerator
	objectGenerator := &ObjectGenerator{EmptyGenerator: EmptyGenerator{K: []byte(""), NullPercentage: 100, T: 6, Out: encoder}, Generators: gen}
	m := bson.M{"key1": 2}
	//m := bson.M{"key1": bson.M{"key3": int64(10)}}

	// , "key2": int32(10), "key3": int64(10), "key4": false, "key5": float64(10), "_id": bson.NewObjectId()
	r, err := bson.Marshal(m)
	if err != nil {
		fmt.Printf("err: %v", err)
		t.Fail()
	}
	fmt.Printf("c array: %v\n", r)

	for i := 0; i < 2; i++ {
		objectGenerator.Value(source)
		//	fmt.Printf("m array: %v\n", encoder.Data)
	}
}
