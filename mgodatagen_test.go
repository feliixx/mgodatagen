package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/stretchr/testify/require"

	"github.com/feliixx/mgodatagen/config"
	"github.com/feliixx/mgodatagen/generators"
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
	Afa        []string      `bson:"afa"`
	Ac         []string      `bson:"ac"`
	Fake       string        `bson:"faker"`
	Cst        int32         `bson:"cst"`
	Nb         int64         `bson:"nb"`
	Nnb        int32         `bson:"nnb"`
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

func TestMain(m *testing.M) {
	s, err := mgo.Dial(URL)
	if err != nil {
		fmt.Printf("couldn't connect to db: %v\n", err)
		os.Exit(connectError)
	}
	defer s.Close()
	generators.ClearRef()
	datagen := &datagen{
		session: s,
		Options: Options{
			Config: Config{
				BatchSize: 1000,
			},
		},
		out: ioutil.Discard,
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

func TestHandleCommandError(t *testing.T) {
	assert := require.New(t)
	r := result{
		Ok: true,
	}
	err := handleCommandError("fail", fmt.Errorf("some reason"), &r)
	assert.Equal("fail\n  cause: some reason", err.Error())

	r = result{
		Ok:     false,
		ErrMsg: "errmsg",
	}
	err = handleCommandError("fail", fmt.Errorf("some reason"), &r)
	assert.Equal("fail\n  cause: errmsg", err.Error())
}

func TestConnectToDb(t *testing.T) {
	assert := require.New(t)

	conn := &Connection{
		Host: "localhost",
		Port: "40000", // should fail
	}

	_, v, err := connectToDB(conn, ioutil.Discard)
	assert.NotNil(err)
	assert.Regexp("^connection failed\n  cause.*", err.Error())

	conn.Port = "27017"

	s, v, err := connectToDB(conn, ioutil.Discard)
	assert.Nil(err)
	assert.True(len(v) > 0)
	s.Close()

	conn = &Connection{
		UserName: "user",
		Password: "pwd",
	}

	_, _, err = connectToDB(conn, ioutil.Discard)
	assert.NotNil(err)
	assert.Regexp("^connection failed\n  cause.*", err.Error())
}

func TestCreateEmptyFile(t *testing.T) {
	assert := require.New(t)

	filename := "testNewFile.json"

	options := &Options{
		Template: Template{
			New: filename,
		},
	}
	err := run(options)
	assert.Nil(err)
	defer os.Remove(filename)

	expected := `[{
"database": "dbName",
"collection": "collectionName",
"count": 1000,
"content": {
    
  }
}]
`
	content, err := ioutil.ReadFile(filename)
	assert.Nil(err)
	assert.Equal(expected, string(content))
}

type distinctResult struct {
	Values []interface{} `bson:"values"`
}

func distinct(dbName, collName, keyName string, result distinctResult) (int, error) {
	err := d.session.DB(dbName).Run(bson.D{
		{Name: "distinct", Value: collName},
		{Name: "key", Value: keyName},
	}, &result)
	if err != nil {
		return 0, err
	}
	return len(result.Values), nil
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
		// string
		assert.InDelta(3, len(r.Name), 17)
		// int32
		assert.InDelta(15, r.C32, 5)
		// int64
		if r.C64 == 0 {
			count++
		} else {
			assert.InDelta(150, r.C64, 50)
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
		assert.InDelta(32, len(r.BinaryData), 8)
		// array
		assert.Equal(3, len(r.List))
		// array of fromArray
		assert.Equal(6, len(r.Afa))
		// array of const
		assert.Equal(2, len(r.Ac))
		// object
		assert.Equal(3, len(r.Object.K1))
		assert.InDelta(-7, r.Object.K2, 3)
		assert.InDelta(5, r.Object.Subob.Sk, 5)
	}
	// null percentage test, allow 2.5% error
	assert.InDelta(100, count, 25)

	dbName := collections[0].DB
	collName := collections[0].Name
	var result distinctResult

	// we expect fixed values for those keys
	testMatrix1 := map[string]int{
		// test maxDistinctValues option
		"name": int(collections[0].Content["name"].MaxDistinctValue),
		// test unique option
		"object.k1": 1000,
		// test value distribution
		"dt":       4,
		"_id":      1000,
		"c32":      11,
		"list":     11,
		"nnb":      1000,
		"nb":       1000,
		"verified": 2,
		"float":    1000,
		"position": 2000,
	}

	for k, v := range testMatrix1 {
		l, err := distinct(dbName, collName, k, result)
		assert.Nil(err)
		assert.Equal(v, l)
	}
	// distinc count may be different from one run to another due
	// to nullPercentage != 0
	testMatrix2 := map[string]int{
		"c64": 80,
	}

	for k, v := range testMatrix2 {
		l, err := distinct(dbName, collName, k, result)
		assert.Nil(err)
		assert.True(l > v)
	}
}

func TestCollectionWithRef(t *testing.T) {
	assert := require.New(t)

	refColl, err := config.CollectionList("samples/config.json")
	assert.Nil(err)

	// TODO : for some reason, the test fails if first collection has more documents
	// than the second collection
	refColl[0].Count = 1000
	refColl[1].Count = 1000

	err = d.createCollection(&refColl[0])
	assert.Nil(err)

	err = d.fillCollection(&refColl[0])
	assert.Nil(err)

	err = d.createCollection(&refColl[1])
	assert.Nil(err)

	err = d.fillCollection(&refColl[1])
	assert.Nil(err)

	var result struct {
		Values []bson.ObjectId `bson:"values"`
		Ok     bool            `bson:"ok"`
	}
	err = d.session.DB(refColl[0].DB).Run(bson.D{
		{Name: "distinct", Value: refColl[0].Name},
		{Name: "key", Value: "_id"},
	}, &result)
	assert.Nil(err)

	c := d.session.DB(refColl[1].DB).C(refColl[1].Name)
	var results []struct {
		ID  bson.ObjectId `bson:"_id"`
		Ref bson.ObjectId `bson:"ref"`
	}
	err = c.Find(nil).All(&results)
	assert.Nil(err)

	for _, doc := range results {
		assert.Contains(result.Values, doc.Ref)
	}
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
	// invalid compression level
	collConfig.CompressionLevel = "unknown"
	err = d.createCollection(collConfig)
	assert.NotNil(err)
	assert.Regexp("^coulnd't create collection with compression level.*\n  cause.*", err.Error())

	// invalid sharded config
	collConfig.CompressionLevel = ""
	collConfig.ShardConfig = config.ShardingConfig{
		ShardCollection: "test.test",
		Key:             bson.M{"_id": 1},
	}

	err = d.createCollection(collConfig)
	assert.NotNil(err)
	assert.Regexp("^wrong value for 'shardConfig.shardCollection'.*", err.Error())

	collConfig.ShardConfig.ShardCollection = collections[0].DB + "." + collections[0].Name
	err = d.createCollection(collConfig)
	assert.NotNil(err)
	assert.Regexp("^couldn't create sharded collection.*", err.Error())

	collConfig.ShardConfig.Key = bson.M{"n": 1}
	err = d.createCollection(collConfig)
	assert.Regexp("^couldn't create sharded collection.*", err.Error())

	collConfig.ShardConfig.Key = bson.M{}
	err = d.createCollection(collConfig)
	assert.NotNil(err)
	assert.Regexp("^wrong value for 'shardConfig.key'.*", err.Error())
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

	indexes[0].Key["c32"] = "invalid"
	err = d.ensureIndex(&collections[0])
	assert.NotNil(err)
	assert.Regexp("^error while building indexes for collection.*\n  cause.*", err.Error())
}

func TestRealRun(t *testing.T) {
	assert := require.New(t)
	generators.ClearRef()
	connOpts := Connection{
		Host: "127.0.0.1",
		Port: "27017",
	}

	options := &Options{
		Connection: connOpts,
		Config: Config{
			ConfigFile:      "samples/config.json",
			NumInsertWorker: 1,
			BatchSize:       100,
		},
	}
	err := run(options)
	assert.Nil(err)

	// should fail because no config file
	options = &Options{
		Connection: connOpts,
		Config: Config{
			NumInsertWorker: 1,
			BatchSize:       1000,
		},
	}
	err = run(options)
	assert.NotNil(err)
	assert.Regexp("^No configuration file provided*", err.Error())
	// should fail because batch size to high
	options = &Options{
		Connection: connOpts,
		Config: Config{
			ConfigFile:      "samples/agg.json",
			NumInsertWorker: 1,
			BatchSize:       10000,
		},
	}
	err = run(options)
	assert.NotNil(err)
	assert.Regexp("^invalid value for -b | --batchsize:*", err.Error())

	options = &Options{
		Connection: connOpts,
		Config: Config{
			ConfigFile:      "samples/agg.json",
			NumInsertWorker: 1,
			BatchSize:       1000,
		},
		General: General{
			Quiet: true,
		},
	}
	err = run(options)
	assert.Nil(err)

	generators.ClearRef()
	// insert 1000 docs
	options = &Options{
		Connection: connOpts,
		Config: Config{
			ConfigFile:      "samples/bson_test.json",
			NumInsertWorker: 1,
			ShortName:       true,
			BatchSize:       1000,
		},
		General: General{
			Quiet: true,
		},
	}
	err = run(options)
	assert.Nil(err)
	// append another 1000 to the same collection
	generators.ClearRef()
	options.Append = true
	err = run(options)
	assert.Nil(err)
	// index only, ie collection is not rebuilt
	options.IndexOnly = true
	err = run(options)
	assert.Nil(err)

	db := d.session.DB("datagen_it_test")

	var r struct {
		N int `bson:"n"`
	}
	command := bson.D{{Name: "count", Value: db.C("test_bson").Name}}
	err = db.Run(command, &r)
	assert.Nil(err)
	assert.Equal(2000, r.N)

}

func TestBulkInsertFail(t *testing.T) {
	assert := require.New(t)

	err := d.createCollection(&collections[0])
	assert.Nil(err)
	collections[0].Count = 11000
	collections[0].Content["_id"] = config.GeneratorJSON{
		Type:     "constant",
		ConstVal: 0,
	}

	err = d.fillCollection(&collections[0])
	assert.NotNil(err)
	assert.Regexp("^exception occurred during bulk insert.*\n  cause.*\n Try.*", err.Error())

}
