package generators_test

import (
	"testing"

	"github.com/feliixx/mgodatagen/datagen/generators"
	"go.mongodb.org/mongo-driver/bson"
)

func TestBigArray(t *testing.T) {

	ci := generators.NewCollInfo(-1, []int{3, 6, 4}, defaultSeed, nil, nil)
	docGenerator, err := ci.NewDocumentGenerator(map[string]generators.Config{
		"key": {
			Type:      generators.TypeArray,
			MinLength: "15",
			MaxLength: "18",
			ArrayContent: &generators.Config{
				Type: generators.TypeBoolean,
			},
		},
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
		if len(a.Key) < 15 || len(a.Key) > 18 {
			t.Errorf("wrong array size, expected between 15 and 18, got %d", len(a.Key))
		}
	}
}

func TestOldSizeAttributeCompat(t *testing.T) {

	ci := generators.NewCollInfo(-1, []int{3, 6, 4}, defaultSeed, nil, nil)
	docGenerator, err := ci.NewDocumentGenerator(map[string]generators.Config{
		"key": {
			Type: generators.TypeArray,
			Size: 2,
			ArrayContent: &generators.Config{
				Type: generators.TypeUUID,
			},
		},
	})
	if err != nil {
		t.Error(err)
	}

	var a struct {
		Key []string `bson:"key"`
	}
	for i := 0; i < 10; i++ {
		err := bson.Unmarshal(docGenerator.Generate(), &a)
		if err != nil {
			t.Error(err)
		}
		if want, got := 2, len(a.Key); want != got {
			t.Errorf("wrong array size, expected %d, got %d", want, got)
		}
	}
}
