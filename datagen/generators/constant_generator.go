package generators

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ConstGenerator for creating constant value. Val already contains the bson element
// type and the key in addition of the actual value
type constGenerator struct {
	base
	strVal  []byte
	bsonVal []byte
}

func newConstantGenerator(base base, value any) (Generator, error) {
	raw, err := bsonValue(string(base.Key()), value)
	if err != nil {
		return nil, err
	}
	// the bson type is already included in raw, so make sure that it's 'unset' from base
	base.bsonType = bson.TypeNull

	return &constGenerator{
		base:    base,
		strVal:  []byte(fmt.Sprint(value)),
		bsonVal: raw,
	}, nil
}

func (g *constGenerator) EncodeValue() {
	g.buffer.Write(g.bsonVal)
}

func (g *constGenerator) EncodeValueAsString() {
	g.buffer.Write(g.strVal)
}

func bsonValue(key string, val any) ([]byte, error) {

	valToMarshal := bson.M{key: val}

	doc, ok := val.(map[string]any)
	if ok && len(doc) == 1 {
		str, ok := doc["$oid"].(string)
		if ok && len(str) == 24 {

			objectId, err := primitive.ObjectIDFromHex(str)
			if err == nil {
				valToMarshal = bson.M{key: objectId}
			}
		}
	}

	raw, err := bson.Marshal(valToMarshal)
	if err != nil {
		return nil, fmt.Errorf("fail to marshal '%s': %v", val, err)
	}
	// remove first 4 bytes (bson document size) and last bytes (terminating 0x00
	// indicating end of document) to keep only the bson content
	return raw[4 : len(raw)-1], nil
}
