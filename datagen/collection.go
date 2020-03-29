package datagen

import (
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo/options"

	"go.mongodb.org/mongo-driver/bson"

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
}

// Index struct used to create an index from `db.runCommand({"createIndexes": "collName", ...})`
type Index struct {
	Name                    string            `bson:"name"`
	Key                     bson.M            `bson:"key"`
	Unique                  bool              `bson:"unique,omitempty"`
	DropDups                bool              `bson:"dropDups,omitempty"`
	Background              bool              `bson:"background,omitempty"`
	Sparse                  bool              `bson:"sparse,omitempty"`
	Bits                    int               `bson:"bits,omitempty"`
	Min                     float64           `bson:"min,omitempty"`
	Max                     float64           `bson:"max,omitempty"`
	BucketSize              float64           `bson:"bucketSize,omitempty"`
	ExpireAfter             int               `bson:"expireAfterSeconds,omitempty"`
	Weights                 bson.M            `bson:"weights,omitempty"`
	DefaultLanguage         string            `bson:"default_language,omitempty"`
	LanguageOverride        string            `bson:"language_override,omitempty"`
	TextIndexVersion        int               `bson:"textIndexVersion,omitempty"`
	PartialFilterExpression bson.M            `bson:"partialFilterExpression,omitempty"`
	Collation               options.Collation `bson:"collation,omitempty"`
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
func ParseConfig(content []byte, ignoreMissingDb bool) ([]Collection, error) {
	var collectionList []Collection
	err := json.Unmarshal(content, &collectionList)
	if err != nil {
		return nil, fmt.Errorf("Error in configuration file: object / array / Date badly formatted: \n\n\t\t%v", err)
	}
	for _, v := range collectionList {
		if v.Name == "" || (v.DB == "" && !ignoreMissingDb) {
			return nil, fmt.Errorf("Error in configuration file: \n\t'collection' and 'database' fields can't be empty")
		}
		if v.Count <= 0 {
			return nil, fmt.Errorf("Error in configuration file: \n\tfor collection %s, 'count' has to be > 0", v.Name)
		}
	}
	return collectionList, nil
}
