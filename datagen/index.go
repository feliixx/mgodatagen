package datagen

import (
	"github.com/iancoleman/orderedmap"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Index struct used to create an index
type Index struct {
	Name string
	// use an ordered map because key order matters for compound index,
	// see https://docs.mongodb.com/manual/core/index-compound/
	Key                     orderedmap.OrderedMap
	Unique                  bool
	Sparse                  bool
	Bits                    int32
	Min                     float64
	Max                     float64
	ExpireAfter             int32 `json:"expireAfterSeconds"`
	Weights                 bson.M
	DefaultLanguage         string
	LanguageOverride        string
	TextIndexVersion        int32
	PartialFilterExpression bson.M
	Collation               options.Collation
	Hidden                  bool
	StorageEngine           bson.M
	WildcardProjection      bson.M
	SphereIndexVersion      int32 `json:"2dsphereIndexVersion"`

	// ignored from mongodb 4.2+
	Background bool
	DropDups   bool

	// deprecated from mongodb 4.9
	BucketSize int32
}

func (idx *Index) ConvertToIndexModel() mongo.IndexModel {

	ordered := bson.D{}
	for _, k := range idx.Key.Keys() {
		v, _ := idx.Key.Get(k)
		ordered = append(ordered, bson.E{Key: k, Value: v})
	}

	opts := &options.IndexOptions{}
	if idx.Name != "" {
		opts = opts.SetName(idx.Name)
	}
	if idx.ExpireAfter != 0 {
		opts = opts.SetExpireAfterSeconds(idx.ExpireAfter)
	}
	if idx.Sparse {
		opts = opts.SetSparse(true)
	}
	if idx.Unique {
		opts = opts.SetUnique(true)
	}
	if idx.TextIndexVersion != 0 {
		opts = opts.SetTextVersion(idx.TextIndexVersion)
	}
	if idx.DefaultLanguage != "" {
		opts = opts.SetDefaultLanguage(idx.DefaultLanguage)
	}
	if idx.LanguageOverride != "" {
		opts = opts.SetLanguageOverride(idx.LanguageOverride)
	}
	if idx.Weights != nil {
		opts = opts.SetWeights(idx.Weights)
	}
	if idx.Bits != 0 {
		opts = opts.SetBits(idx.Bits)
	}
	if idx.Max != 0 {
		opts = opts.SetMax(idx.Max)
	}
	if idx.Min != 0 {
		opts = opts.SetMin(idx.Min)
	}
	if idx.BucketSize != 0 {
		opts = opts.SetBucketSize(idx.BucketSize)
	}
	if idx.PartialFilterExpression != nil {
		opts = opts.SetPartialFilterExpression(idx.PartialFilterExpression)
	}
	if idx.Collation.Locale != "" {
		opts = opts.SetCollation(&idx.Collation)
	}
	if idx.SphereIndexVersion != 0 {
		opts = opts.SetSphereVersion(idx.SphereIndexVersion)
	}
	if idx.Hidden {
		opts = opts.SetHidden(true)
	}
	if idx.StorageEngine != nil {
		opts = opts.SetStorageEngine(idx.StorageEngine)
	}
	if idx.WildcardProjection != nil {
		opts = opts.SetWildcardProjection(idx.WildcardProjection)
	}
	return mongo.IndexModel{
		Keys:    ordered,
		Options: opts,
	}
}
