package generators_test

import (
	"testing"

	"github.com/feliixx/mgodatagen/datagen/generators"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestDocumentWithDecimal128(t *testing.T) {

	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed, nil, nil)
	docGenerator, err := ci.NewDocumentGenerator(map[string]generators.Config{
		"key": {Type: generators.TypeDecimal},
	})
	if err != nil {
		t.Error(err)
	}

	var d struct {
		Key primitive.Decimal128 `bson:"key"`
	}
	for i := 0; i < 10; i++ {
		err := bson.Unmarshal(docGenerator.Generate(), &d)
		if err != nil {
			t.Error(err)
		}

		if d.Key == primitive.NewDecimal128(0, 0) {
			t.Error("expected a non nil decimal 128")
		}
	}
}
