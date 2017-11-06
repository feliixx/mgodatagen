package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/stretchr/testify/require"

	"github.com/feliixx/mgodatagen/config"
)

const (
	URL          = "mongodb://"
	connectError = 1
	configError  = 2
	dateFormat   = "2006-Jan-02"
)

var (
	collections []config.Collection
	d           *datagen
)

type expectedDoc struct {
	ID         bson.ObjectId `bson:"_id"`
	Name       string        `bson:"name"`
	C32        int32         `bson:"c32"`
	C64        int64         `bson:"c64"`
	Float      float64       `bson:"float"`
	Verified   bool          `bson:"verified"`
	Position   []float64     `bson:"position"`
	Dt         string        `bson:"dt"`
	Fake       string        `bson:"faker"`
	Cst        int32         `bson:"cst"`
	Nb         int64         `bson:"nb"`
	Date       time.Time     `bson:"date"`
	BinaryData []byte        `bson:"binaryData"`
	List       []int32       `bson:"list"`
	Object     struct {
		K1    string `bson:"k1"`
		K2    int32  `bson:"k2"`
		Subob struct {
			Sk int32 `bson:"s-k"`
		} `bson:"sub-ob"`
	} `bson:"object"`
}

type logger struct{}

// redirect progress bar and non usefull info to avoid polluting stderr
func (l logger) Write(b []byte) (int, error) {
	return 0, nil
}

func TestMain(m *testing.M) {
	s, err := mgo.Dial(URL)
	if err != nil {
		fmt.Printf("couldn't connect to db: %v\n", err)
		os.Exit(connectError)
	}
	defer s.Close()
	datagen := &datagen{
		session: s,
		Options: Options{},
		out:     logger{},
	}
	d = datagen

	c, err := config.CollectionList("samples/bson_test.json")
	if err != nil {
		fmt.Printf("error in config file: %v\n", err)
		os.Exit(configError)
	}
	collections = c
	retCode := m.Run()

	err = s.DB(collections[0].DB).DropDatabase()
	if err != nil {
		fmt.Printf("couldn't drop db: %v\n", err)
		os.Exit(connectError)
	}
	os.Exit(retCode)
}

func TestCollectionContent(t *testing.T) {
	assert := require.New(t)

	err := d.createCollection(&collections[0])
	assert.Nil(err)

	err = d.fillCollection(&collections[0])
	assert.Nil(err)

	c := d.session.DB(collections[0].DB).C(collections[0].Name)
	docCount, err := c.Count()
	assert.Nil(err)
	assert.Equal(docCount, int(collections[0].Count))

	var results []expectedDoc
	err = c.Find(nil).All(&results)
	assert.Nil(err)
	count := 0

	fromArr := []string{
		"2012-10-10",
		"2012-12-12",
		"2014-01-01",
		"2016-05-05",
	}

	expectedDate, _ := time.Parse(dateFormat, "2014-Jan-01")

	for i, r := range results {
		// int32
		assert.InDelta(6, len(r.Name), 2)
		// int64
		assert.InDelta(15, r.C32, 5)
		if r.C64 == 0 {
			count++
		} else {
			assert.InDelta(15000, r.C64, 5000)
		}
		// float
		assert.InDelta(5, r.Float, 5)
		// position testing
		assert.InDelta(0, r.Position[0], 90)
		assert.InDelta(0, r.Position[1], 180)
		// fromArray
		assert.Contains(fromArr, r.Dt)
		// cst
		assert.Equal(int32(2), r.Cst)
		// autoincrement
		assert.Equal(int64(i), r.Nb)
		// Date
		assert.WithinDuration(expectedDate, r.Date, time.Second*60*60*24*365*4)
		// binary data
		assert.Equal(24, len(r.BinaryData))
		// array
		assert.Equal(3, len(r.List))
		// object
		assert.Equal(3, len(r.Object.K1))
		assert.InDelta(-7, r.Object.K2, 3)
		assert.InDelta(5, r.Object.Subob.Sk, 5)
	}
	// null precentage test
	assert.InDelta(100, count, 25)

	var result struct {
		Values []string `bson:"values"`
		Ok     bool     `bson:"ok"`
	}
	// test maxDistinctValues option
	err = d.session.DB(collections[0].DB).Run(bson.D{
		{Name: "distinct", Value: collections[0].Name},
		{Name: "key", Value: "name"},
	}, &result)
	assert.Nil(err)
	assert.Equal(collections[0].Content["name"].MaxDistinctValue, len(result.Values))
	// test unique option
	err = d.session.DB(collections[0].DB).Run(bson.D{
		{Name: "distinct", Value: collections[0].Name},
		{Name: "key", Value: "object.k1"},
	}, &result)
	assert.Nil(err)
	assert.Equal(1000, len(result.Values))
}

func TestCollectionContentWithAggregation(t *testing.T) {
	assert := require.New(t)

	aggColl, err := config.CollectionList("samples/agg.json")
	assert.Nil(err)

	err = d.createCollection(&aggColl[0])
	assert.Nil(err)

	err = d.fillCollection(&aggColl[0])
	assert.Nil(err)

	err = d.createCollection(&aggColl[1])
	assert.Nil(err)

	err = d.fillCollection(&aggColl[1])
	assert.Nil(err)

	c := d.session.DB(aggColl[1].DB).C(aggColl[1].Name)
	var results []bson.M
	err = c.Find(nil).All(&results)
	assert.Nil(err)

	possibleValues := []string{"a", "b", "e", "d", "c", "h", "f", "g", "i"}

	for _, r := range results {
		b := r["AG-CI"].(bson.M)
		assert.InDelta(50, b["m"], 50)
		assert.InDelta(9950, b["M"], 50)

		assert.InDelta(1650, r["AG-FI"], 200)

		a := r["AG-VA"].([]interface{})
		assert.True(len(a) > 0)

		for _, v := range a {
			assert.Contains(possibleValues, v)
		}
	}
}

func TestCreateCollection(t *testing.T) {
	assert := require.New(t)

	collConfig := &config.Collection{
		DB:               collections[0].DB,
		Name:             collections[0].Name,
		Count:            1,
		CompressionLevel: "zlib",
	}

	err := d.createCollection(collConfig)
	assert.Nil(err)

	var result struct {
		WiredTiger struct {
			CreationString string `bson:"creationString"`
		} `bson:"wiredTiger"`
	}
	err = d.session.DB(collections[0].DB).Run(bson.D{{Name: "collStats", Value: collections[0].Name}}, &result)
	assert.Nil(err)
	assert.Contains(result.WiredTiger.CreationString, "block_compressor=zlib")
}

func TestCollectionWithIndexes(t *testing.T) {
	assert := require.New(t)

	err := d.createCollection(&collections[0])
	assert.Nil(err)

	err = d.fillCollection(&collections[0])
	assert.Nil(err)

	indexes := []config.Index{
		{
			Name: "idx_1",
			Key:  bson.M{"c32": 1},
		},
		{
			Name: "idx_2",
			Key:  bson.M{"c64": -1},
		},
	}

	collections[0].Indexes = indexes
	err = d.ensureIndex(&collections[0])
	assert.Nil(err)

	c := d.session.DB(collections[0].DB).C(collections[0].Name)
	idx, err := c.Indexes()
	assert.Nil(err)

	assert.Equal(len(idx), len(indexes)+1)
	assert.Equal(indexes[0].Name, idx[1].Name)
	assert.Equal(indexes[1].Name, idx[2].Name)
}
