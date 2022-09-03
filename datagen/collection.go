package datagen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/feliixx/mgodatagen/datagen/generators"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
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
	decoder := json.NewDecoder(bytes.NewReader(rewriteForBackwardCompatibility(content)))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()

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

var (
	regMin = regexp.MustCompile(`"(minInt|minLong|minDouble)"`)
	regMax = regexp.MustCompile(`"(maxInt|maxLong|maxDouble)"`)
)

func rewriteForBackwardCompatibility(content []byte) []byte {

	result := regMin.ReplaceAll(content, []byte(`"min"`))
	return regMax.ReplaceAll(result, []byte(`"max"`))
}
