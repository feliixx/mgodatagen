package generators_test

import (
	"testing"

	"github.com/feliixx/mgodatagen/datagen/generators"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestDocumentWithValidConstantObjectID(t *testing.T) {
	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed, nil, nil)
	docGenerator, err := ci.NewDocumentGenerator(map[string]generators.Config{
		"key": {
			Type: generators.TypeConstant,
			ConstVal: bson.M{
				"$oid": "5a934e000102030405000001",
			},
		},
	})
	if err != nil {
		t.Error(err)
	}

	var d struct {
		Key primitive.ObjectID `bson:"key"`
	}
	for i := 0; i < 10; i++ {
		err := bson.Unmarshal(docGenerator.Generate(), &d)
		if err != nil {
			t.Error(err)
		}

		if d.Key == [12]byte{} {
			t.Error("objectId is nil")
		}
	}
}

func TestDocumentWithInvalidConstantObjectID(t *testing.T) {
	ci := generators.NewCollInfo(1, []int{3, 6, 4}, defaultSeed, nil, nil)
	docGenerator, err := ci.NewDocumentGenerator(map[string]generators.Config{
		"key": {
			Type: generators.TypeConstant,
			ConstVal: bson.M{
				"$oid": "5a9",
			},
		},
	})
	if err != nil {
		t.Error(err)
	}

	var d struct {
		Key struct {
			Oid string `bson:"$oid"`
		} `bson:"key"`
	}
	for i := 0; i < 10; i++ {
		err := bson.Unmarshal(docGenerator.Generate(), &d)
		if err != nil {
			t.Error(err)
		}

		if d.Key.Oid != "5a9" {
			t.Errorf("expected '5a9' but got '%s'", d.Key.Oid)
		}
	}
}