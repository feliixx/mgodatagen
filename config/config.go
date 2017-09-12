package config

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
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
	Content map[string]GeneratorJSON `json:"content"`
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
	Name                    string         `bson:"name"`
	Key                     bson.M         `bson:"key"`
	Unique                  bool           `bson:"unique,omitempty"`
	DropDups                bool           `bson:"dropDups,omitempty"`
	Background              bool           `bson:"background,omitempty"`
	Sparse                  bool           `bson:"sparse,omitempty"`
	Bits                    int            `bson:"bits,omitempty"`
	Min                     float64        `bson:"min,omitempty"`
	Max                     float64        `bson:"max,omitempty"`
	BucketSize              float64        `bson:"bucketSize,omitempty"`
	ExpireAfter             int            `bson:"expireAfterSeconds,omitempty"`
	Weights                 bson.M         `bson:"weights,omitempty"`
	DefaultLanguage         string         `bson:"default_language,omitempty"`
	LanguageOverride        string         `bson:"language_override,omitempty"`
	TextIndexVersion        int            `bson:"textIndexVersion,omitempty"`
	PartialFilterExpression bson.M         `bson:"partialFilterExpression,omitempty"`
	Collation               *mgo.Collation `bson:"collation,omitempty"`
}

// ShardingConfig struct that holds information to shard the collection
type ShardingConfig struct {
	ShardCollection  string         `bson:"shardCollection"`
	Key              bson.M         `bson:"key"`
	unique           bool           `bson:"unique"`
	NumInitialChunks int            `bson:"numInitialChunks,omitempty"`
	Collation        *mgo.Collation `bson:"collation,omitempty"`
}

// GeneratorJSON struct containing all possible options
type GeneratorJSON struct {
	// Type of object to genereate.
	Type string `json:"type"`
	// Percentage of documents that won't contains this field
	NullPercentage int64 `json:"nullPercentage"`
	// Maximum number of distinct value for this field
	MaxDistinctValue int `json:"maxDistinctValue"`
	// For `string` type only. If set to 'true', string will be unique
	Unique bool `json:"unique"`
	// For `string` and `binary` type only. Specify the Min length of the object to generate
	MinLength int32 `json:"MinLength"`
	// For `string` and `binary` type only. Specify the Max length of the object to generate
	MaxLength int32 `json:"MaxLength"`
	// For `int` type only. Lower bound for the int32 to generate
	MinInt32 int32 `json:"MinInt"`
	// For `int` type only. Higher bound for the int32 to generate
	MaxInt32 int32 `json:"MaxInt"`
	// For `long` type only. Lower bound for the int64 to generate
	MinInt64 int64 `json:"MinLong"`
	// For `long` type only. Higher bound for the int64 to generate
	MaxInt64 int64 `json:"MaxLong"`
	// For `double` type only. Lower bound for the float64 to generate
	MinFloat64 float64 `json:"MinDouble"`
	// For `double` type only. Higher bound for the float64 to generate
	MaxFloat64 float64 `json:"MaxDouble"`
	// For `array` only. Size of the array
	Size int `json:"size"`
	// For `array` only. GeneratorJSON to fill the array. Need to
	// pass a pointer here to avoid 'invalid recursive type' error
	ArrayContent *GeneratorJSON `json:"arrayContent"`
	// For `object` only. List of GeneratorJSON to generate the content
	// of the object
	ObjectContent map[string]GeneratorJSON `json:"objectContent"`
	// For `fromArray` only. If specified, the generator pick one of the item of the array
	In []interface{} `json:"in"`
	// For `date` only. Lower bound for the date to generate
	StartDate time.Time `json:"StartDate"`
	// For `date` only. Higher bound for the date to generate
	EndDate time.Time `json:"endDate"`
	// For `constant` type only. Value of the constant field
	ConstVal interface{} `json:"constVal"`
	// For `autoincrement` type only. Start value
	Start32 int32 `json:"startInt"`
	// For `autoincrement` type only. Start value
	Start64 int64 `json:"startLong"`
	// For `autoincrement` type only. Start value
	AutoType string `json:"autoType"`
	// For `ref` type only. Used to retrieve the array storing the value
	// for this field
	ID int `json:"id"`
	// For `ref` type only. generator for the field
	RefContent *GeneratorJSON `json:"refContent"`
	// For `countAggregator` and `valueAggregator` only
	Collection string `json:"collection"`
	// For `countAggregator` and `valueAggregator` onl
	Database string `json:"database"`
	// For `countAggregator` and `valueAggregator` only
	Field string `json:"field"`
	// For `countAggregator` and `valueAggregator` only
	Query bson.M `json:"query"`
}
