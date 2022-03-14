package datagen

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/iancoleman/orderedmap"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/feliixx/mgodatagen/datagen/generators"
)

// Collection struct storing global collection info
type Collection struct {
	// Database to use
	DB string `json:"database"`
	// Collection name in the database
	Name string `json:"collection"`
	// Number of documents to insert in the collection
	Count int `json:"count"`
	// Schema of the documents for this collection
	Content map[string]generators.Config `json:"content"`
	// Compression level for a collection. Available for `WiredTiger` only.
	// can be none|snappy|zlib. Default is "snappy"
	CompressionLevel string `json:"compressionLevel"`
	// List of indexes to build on this collection
	Indexes []Index `json:"indexes"`
	// Sharding information for sharded collection
	ShardConfig ShardingConfig `json:"shardConfig"`

	docGenerator *generators.DocumentGenerator
	aggregators  []generators.Aggregator
}

// Index struct used to create an index from `db.runCommand({"createIndexes": "collName", ...})`
type Index struct {
	Name string `bson:"name"`
	// use an ordered map because key order matters for compound index,
	// see https://docs.mongodb.com/manual/core/index-compound/
	Key                     orderedmap.OrderedMap `bson:"key"`
	Unique                  bool                  `bson:"unique,omitempty"`
	Sparse                  bool                  `bson:"sparse,omitempty"`
	Bits                    int32                 `bson:"bits,omitempty"`
	Min                     float64               `bson:"min,omitempty"`
	Max                     float64               `bson:"max,omitempty"`
	BucketSize              int32                 `bson:"bucketSize,omitempty"`
	ExpireAfter             int32                 `bson:"expireAfterSeconds,omitempty" json:"expireAfterSeconds"`
	Weights                 bson.M                `bson:"weights,omitempty"`
	DefaultLanguage         string                `bson:"default_language,omitempty"`
	LanguageOverride        string                `bson:"language_override,omitempty"`
	TextIndexVersion        int32                 `bson:"textIndexVersion,omitempty"`
	PartialFilterExpression bson.M                `bson:"partialFilterExpression,omitempty"`
	Collation               options.Collation     `bson:"collation,omitempty"`

	// ignored from mongodb 4.2+
	Background bool `bson:"background,omitempty"`
	DropDups   bool `bson:"dropDups,omitempty"`
}

// ShardingConfig struct that holds information to shard the collection
type ShardingConfig struct {
	ShardCollection  string            `bson:"shardCollection"`
	Key              bson.M            `bson:"key"`
	NumInitialChunks int               `bson:"numInitialChunks,omitempty"`
	Collation        options.Collation `bson:"collation,omitempty"`
}

// ParseConfig returns a list of Collection to create from a
// json configuration file
func ParseConfig(content []byte, ignoreMissingDb bool) (collections []Collection, err error) {

	// Use a decoder here se we can disallow unknow fields. Decode will return an
	// error if some fields from content can't be matched
	// this should help detect typos / spelling errors in config files
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()

	err = decoder.Decode(&collections)
	if err != nil {
		return nil, fmt.Errorf("error in configuration file: object / array / Date badly formatted: \n\n\t\t%v", err)
	}
	for _, c := range collections {
		if c.Name == "" || (c.DB == "" && !ignoreMissingDb) {
			return nil, fmt.Errorf("error in configuration file: \n\t'collection' and 'database' fields can't be empty")
		}
		if c.Count <= 0 {
			return nil, fmt.Errorf("error in configuration file: \n\tfor collection %s, 'count' has to be > 0", c.Name)
		}
	}
	return collections, nil
}
