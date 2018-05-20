package datagen_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"

	"github.com/feliixx/mgodatagen/datagen"
)

var (
	session         *mgo.Session
	defaultConnOpts = datagen.Connection{
		Host: "127.0.0.1",
		Port: "27017",
	}
	defaultGeneralOpts = datagen.General{
		Quiet: true,
	}
)

func TestMain(m *testing.M) {
	s, err := mgo.Dial("mongodb://127.0.0.1:27017")
	if err != nil {
		fmt.Printf("couldn't connect to db: %v\n", err)
		os.Exit(1)
	}
	defer s.Close()
	session = s

	retCode := m.Run()

	err = s.DB("datagen_it_test").DropDatabase()
	if err != nil {
		fmt.Printf("couldn't drop db: %v\n", err)
		os.Exit(1)
	}
	os.Exit(retCode)
}

func TestCreateEmptyFile(t *testing.T) {

	filename := "testNewFile.json"

	options := &datagen.Options{
		Template: datagen.Template{
			New: filename,
		},
	}
	err := datagen.Generate(options, ioutil.Discard)
	if err != nil {
		t.Errorf("expected no error for creating empty file but got %v", err)
	}
	defer os.Remove(filename)

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Error(err)
	}

	want := `[{
"database": "dbName",
"collection": "collectionName",
"count": 1000,
"content": {
    
  }
}]
`
	if got := string(content); want != got {
		t.Errorf("expected \n%s\nbut got\n%s", want, got)
	}
}

func TestProgressOutput(t *testing.T) {

	configFile := "testdata/empty.json"
	options := defaultOpts(configFile)
	options.Quiet = false

	var buffer bytes.Buffer
	err := datagen.Generate(&options, &buffer)
	if err != nil {
		t.Error(err)
	}

	b, err := ioutil.ReadFile("testdata/output.txt")
	if err != nil {
		t.Error(err)
	}
	expectedLines := bytes.Split(b, []byte("\n"))

	outputLines := bytes.Split(buffer.Bytes(), []byte("\n"))
	// do not check line 1 and n-1 as they depends on MongoDB version
	// and elapsed time respectively
	for i, want := range expectedLines {
		if i == 1 || i == len(expectedLines)-1 {
			continue
		}
		got := outputLines[i]
		if !bytes.Equal(want, got) {
			t.Errorf("for output line %d, expected \n%s \n but got \n%s", i, want, got)
		}
	}
}

func TestCollectionContent(t *testing.T) {

	configFile := "generators/testdata/full-bson.json"
	collections := parseConfig(t, configFile)
	options := defaultOpts(configFile)
	err := datagen.Generate(&options, ioutil.Discard)
	if err != nil {
		t.Error(err)
	}

	c := session.DB(collections[0].DB).C(collections[0].Name)
	docCount, err := c.Count()
	if err != nil {
		t.Error(err)
	}
	if want, got := int(collections[0].Count), docCount; want != got {
		t.Errorf("expected %d documents but got %d", want, got)
	}

	var results []struct {
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
	err = c.Find(nil).All(&results)
	if err != nil {
		t.Error(err)
	}

	fromArr := sort.StringSlice{"2012-10-10", "2012-12-12", "2014-01-01", "2016-05-05"}

	df := "2006-Jan-02"
	expectedDate, err := time.Parse(df, "2014-Jan-01")
	if err != nil {
		t.Error(err)
	}
	count := 0
	for i, r := range results {
		// string
		if len(r.Name) < 15 || len(r.Name) > 20 {
			t.Errorf("'name' should be 15 < name < 20 but was %d", len(r.Name))
		}
		// int32
		if r.C32 < 10 || r.C32 > 20 {
			t.Errorf("'c32' should be 10 < c32 < 20 but was %d", r.C64)
		}
		// int64
		if r.C64 == 0 {
			count++
		} else {
			if r.C64 < 100 || r.C64 > 200 {
				t.Errorf("'c64' should be 100 < c64 < 200 but was %d", r.C64)
			}
		}
		// float
		if r.Float < 0 || r.Float > 10 {
			t.Errorf("'float' should be 0 < float < 10, but was %f", r.Float)
		}
		// position testing
		if r.Position[0] < -90 || r.Position[0] > 90 {
			t.Errorf("'pos[0]' should be -90 < pos[0] < 90, but was %f", r.Position[0])
		}
		if r.Position[1] < -180 || r.Position[1] > 180 {
			t.Errorf("'pos[1]' should be -180 < pos[1] < 180, but was %f", r.Position[1])
		}
		// fromArray
		idx := fromArr.Search(r.Dt)
		if idx == len(fromArr) || fromArr[idx] != r.Dt {
			t.Errorf("'dt' should be on of the values of fromArray, but was %s", r.Dt)
		}
		// cst
		if r.Cst != int32(2) {
			t.Errorf("'cst' field should be int32(2), but was %v", r.Cst)
		}
		// autoincrement
		if r.Nb != int64(i) {
			t.Errorf("'nb' field should be %d, but was %d", int64(i), r.Nb)
		}
		// Date
		dt := expectedDate.Sub(r.Date)
		delta := time.Second * 60 * 60 * 24 * 365 * 4
		if dt < -delta || dt > delta {
			t.Errorf("'date' should be within %v, but was %v", delta, r.Date)
		}
		// binary data
		if len(r.BinaryData) < 24 || len(r.BinaryData) > 40 {
			t.Errorf("'binaryData' len should be 4 < len < 40, but was %d", len(r.BinaryData))
		}
		// array
		if len(r.List) != 3 {
			t.Errorf("'list' should have 3 items but got only %d", len(r.List))
		}
		// array of fromArray
		if len(r.Afa) != 6 {
			t.Errorf("'afa' should have 6 items but got only %d", len(r.Afa))
		}
		// array of const
		if len(r.Ac) != 2 {
			t.Errorf("'ac' should have 2 items but got only %d", len(r.Ac))
		}
		// object
		if len(r.Object.K1) != 3 {
			t.Errorf("'object.k1' should have 3 items but got only %d", len(r.Object.K1))
		}
		if r.Object.K2 < -10 || r.Object.K2 > -5 {
			t.Errorf("'object.k2' should be -10 < object.k2 < -5, but was %d", r.Object.K2)
		}
		if r.Object.Subob.Sk < 0 || r.Object.Subob.Sk > 10 {
			t.Errorf("'object.subob.sk' should be 0 < object.subob < 10, but was %d", r.Object.Subob.Sk)
		}
	}
	// null percentage test, allow 2.5% error
	if count < 75 || count > 125 {
		t.Errorf("doc nb with c64 == null should be 75 < count < 125 (2.5percent) but was %d", count)
	}

	// we expect fixed values for those keys
	maxDistinctValuesTests := map[string]int{
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
	var result distinctResult
	for key, value := range maxDistinctValuesTests {
		nb := distinct(t, collections[0].DB, collections[0].Name, key, result)
		if value != nb {
			t.Errorf("for field %s, expected %d distinct values but got %d", key, value, nb)
		}
	}
	// distinct count may be different from one run to another due
	// to nullPercentage != 0
	maxDistinctValuesNullPercentageTests := map[string]int{
		"c64": 80,
	}

	for key, value := range maxDistinctValuesNullPercentageTests {
		nb := distinct(t, collections[0].DB, collections[0].Name, key, result)
		if value > nb {
			t.Errorf("for field %s, expected %d max distinct values but got %d", key, value, nb)
		}
	}
}

func TestCollectionWithRef(t *testing.T) {

	configFile := "generators/testdata/ref.json"
	collections := parseConfig(t, configFile)
	options := defaultOpts(configFile)
	err := datagen.Generate(&options, ioutil.Discard)
	if err != nil {
		t.Error(err)
	}

	var distinct struct {
		Values []bson.ObjectId `bson:"values"`
		Ok     bool            `bson:"ok"`
	}

	err = session.DB(collections[0].DB).Run(bson.D{
		{Name: "distinct", Value: collections[0].Name},
		{Name: "key", Value: "_id"},
	}, &distinct)
	if err != nil {
		t.Error(err)
	}

	c := session.DB(collections[1].DB).C(collections[1].Name)
	var result []struct {
		ID  bson.ObjectId `bson:"_id"`
		Ref bson.ObjectId `bson:"ref"`
	}
	err = c.Find(nil).Sort("_id").All(&result)
	if err != nil {
		t.Error(err)
	}

	for _, r := range distinct.Values {
		i := sort.Search(len(result), func(i int) bool { return result[i].Ref.String() >= r.String() })
		if i == len(result) || result[i].Ref.String() != r.String() {
			t.Errorf("%v not found in result", r.String())
		}
	}
}

func TestCollectionContentWithAggregation(t *testing.T) {

	configFile := "generators/testdata/full-aggregation.json"
	collections := parseConfig(t, configFile)
	options := defaultOpts(configFile)
	err := datagen.Generate(&options, ioutil.Discard)
	if err != nil {
		t.Error(err)
	}

	c := session.DB(collections[1].DB).C(collections[1].Name)
	var results []bson.M
	err = c.Find(nil).All(&results)
	if err != nil {
		t.Error(err)
	}

	possibleValues := sort.StringSlice{"a", "b", "c", "d", "e", "f", "g", "h", "i"}

	for _, r := range results {
		b := r["AG-CI"].(bson.M)
		m := b["m"].(int)
		if m < 0 || m > 100 {
			t.Errorf("'m' field sould be 0 < m < 100, but was %d", m)
		}
		mM := b["M"].(int)
		if mM < 9900 || mM > 10000 {
			t.Errorf("'M' field sould be 900 < M < 1000, but was %d", mM)
		}
		agFI := r["AG-FI"].(int)
		if agFI < 1450 || agFI > 1850 {
			t.Errorf("'AG-FI' field sould be 1450 < AG-FI < 1850, but was %d", agFI)
		}

		vv := r["AG-VA"].([]interface{})
		if len(vv) == 0 {
			t.Errorf("'AG-VA' field should be a non empty array, but was %v", vv)
		}
		for _, v := range vv {
			v := v.(string)
			i := possibleValues.Search(v)
			if i == len(possibleValues) || possibleValues[i] != v {
				t.Errorf("got an unexpected value: %s", v)
			}
		}
	}
}

func TestCollectionCompression(t *testing.T) {

	createCollectionTests := []struct {
		name             string
		options          datagen.Options
		compressionLevel string
		errMsgRegex      *regexp.Regexp
		correct          bool
	}{

		{
			name:             "zlib compressor",
			options:          defaultOpts("testdata/zlib.json"),
			compressionLevel: "zlib",
			errMsgRegex:      nil,
			correct:          true,
		},
		{
			name:             "invalid compressor",
			options:          defaultOpts("testdata/invalid-compression.json"),
			compressionLevel: "",
			errMsgRegex:      regexp.MustCompile("^coulnd't create collection with compression level.*\n  cause.*"),
			correct:          false,
		},
		{
			name:             "invalid compressor",
			options:          defaultOpts("testdata/empty.json"),
			compressionLevel: "snappy",
			errMsgRegex:      nil,
			correct:          true,
		},
	}

	var result struct {
		WiredTiger struct {
			CreationString string `bson:"creationString"`
		} `bson:"wiredTiger"`
	}

	for _, tt := range createCollectionTests {
		t.Run(tt.name, func(t *testing.T) {
			collections := parseConfig(t, tt.options.ConfigFile)
			err := datagen.Generate(&tt.options, ioutil.Discard)
			if tt.correct {
				if err != nil {
					t.Errorf("expected no error for config %v: \n%v", tt.options, err)
				}
				err = session.DB(collections[0].DB).Run(bson.D{{Name: "collStats", Value: collections[0].Name}}, &result)
				if err != nil {
					t.Error(err)
				}
				if !strings.Contains(result.WiredTiger.CreationString, "block_compressor="+tt.compressionLevel) {
					t.Errorf("block_compressor should be %s, but result was %s", tt.compressionLevel, result.WiredTiger.CreationString)
				}
			} else {
				if err == nil {
					t.Errorf("expected an error for config %v", tt.options)
				}
				if !tt.errMsgRegex.MatchString(err.Error()) {
					t.Errorf("error should match regex %s, but was %v", tt.errMsgRegex.String(), err)
				}
			}
		})
	}
}

func TestCollectionWithIndexes(t *testing.T) {

	createCollectionWithIndexesTests := []struct {
		name        string
		configFile  string
		indexes     []datagen.Index
		correct     bool
		errMsgRegex *regexp.Regexp
	}{
		{
			name:       "valid index",
			configFile: "testdata/index.json",
			indexes: []datagen.Index{
				{
					Name: "idx_1",
					Key:  bson.M{"c32": 1},
				},
				{
					Name: "idx_2",
					Key:  bson.M{"c64": -1},
				},
			},
			correct:     true,
			errMsgRegex: nil,
		},
		{
			name:       "invalid index",
			configFile: "testdata/invalid-index.json",
			indexes: []datagen.Index{
				{
					Name: "idx_1",
					Key:  bson.M{"c32": 1},
				},
				{
					Name: "idx_2",
					Key:  bson.M{"invalid": "invalid"},
				},
			},
			correct:     false,
			errMsgRegex: regexp.MustCompile("^error while building indexes for collection.*\n  cause.*"),
		},
	}

	for _, tt := range createCollectionWithIndexesTests {
		t.Run(tt.name, func(t *testing.T) {
			collections := parseConfig(t, tt.configFile)
			options := defaultOpts(tt.configFile)
			err := datagen.Generate(&options, ioutil.Discard)
			if tt.correct {
				if err != nil {
					t.Errorf("ensureIndex with indexes %v should not fail: \n%v", tt.indexes, err)
				}
				c := session.DB(collections[0].DB).C(collections[0].Name)
				idx, err := c.Indexes()
				if err != nil {
					t.Errorf("fail to get indexes: %v", err)
				}
				for i := range tt.indexes {
					// idx[0] is index on '_id' field
					if want, got := tt.indexes[i].Name, idx[i+1].Name; want != got {
						t.Errorf("index does not match: expected %s, got %s", want, got)
					}
				}
			} else {
				if err == nil {
					t.Errorf("expected an error for indexes %v", tt.indexes)
				}
				if !tt.errMsgRegex.MatchString(err.Error()) {
					t.Errorf("error message should match %s, but was %v", tt.errMsgRegex.String(), err)
				}
			}
		})
	}
}

func TestGenerate(t *testing.T) {

	realRunTests := []struct {
		name          string
		options       datagen.Options
		correct       bool
		errMsgRegex   *regexp.Regexp
		expectedNbDoc int
	}{
		{
			name:          "full-bson.json",
			options:       defaultOpts("generators/testdata/full-bson.json"),
			correct:       true,
			errMsgRegex:   nil,
			expectedNbDoc: 1000,
		},
		{
			name: "append mode",
			options: datagen.Options{
				Connection: defaultConnOpts,
				Configuration: datagen.Configuration{
					ConfigFile: "generators/testdata/full-bson.json",
					BatchSize:  1000,
					Append:     true,
				},
				General: defaultGeneralOpts,
			},
			correct:       true,
			errMsgRegex:   nil,
			expectedNbDoc: 2000,
		},
		{
			name: "print version",
			options: datagen.Options{
				Connection: defaultConnOpts,
				Configuration: datagen.Configuration{
					ConfigFile: "generators/testdata/full-bson.json",
					BatchSize:  1000,
				},
				General: datagen.General{
					Version: true,
					Quiet:   true,
				},
			},
			correct:       true,
			errMsgRegex:   nil,
			expectedNbDoc: 2000,
		},
		{
			name: "index only",
			options: datagen.Options{
				Connection: defaultConnOpts,
				Configuration: datagen.Configuration{
					ConfigFile:      "generators/testdata/full-bson.json",
					NumInsertWorker: 1,
					BatchSize:       1000,
					IndexOnly:       true,
				},
				General: defaultGeneralOpts,
			},
			correct:       true,
			errMsgRegex:   nil,
			expectedNbDoc: 2000,
		},
		{
			name:        "no config file",
			options:     defaultOpts(""),
			correct:     false,
			errMsgRegex: regexp.MustCompile("^No configuration file provided*"),
		},
		{
			name: "invalid batch size",
			options: datagen.Options{
				Connection: defaultConnOpts,
				Configuration: datagen.Configuration{
					ConfigFile:      "testdata/empty.json",
					NumInsertWorker: 1,
					BatchSize:       1001,
				},
				General: defaultGeneralOpts,
			},
			correct:     false,
			errMsgRegex: regexp.MustCompile("^invalid value for -b | --batchsize:*"),
		},
		{
			name:        "full-aggregation.json",
			options:     defaultOpts("generators/testdata/full-aggregation.json"),
			correct:     true,
			errMsgRegex: nil,
		},
		{
			name: "non-listenning port",
			options: datagen.Options{
				Connection: datagen.Connection{
					Host:    "localhost",
					Port:    "40000",
					Timeout: 500 * time.Millisecond,
				},
				Configuration: datagen.Configuration{
					ConfigFile: "testdata/empty.json",
					BatchSize:  1000,
				},
				General: defaultGeneralOpts,
			},
			correct:     false,
			errMsgRegex: regexp.MustCompile("^connection failed\n  cause.*"),
		},
		{
			name: "auth not enabled on db",
			options: datagen.Options{
				Connection: datagen.Connection{
					UserName: "user",
					Password: "pwd",
					Timeout:  500 * time.Millisecond,
				},
				Configuration: datagen.Configuration{
					ConfigFile: "testdata/empty.json",
					BatchSize:  1000,
				},
				General: defaultGeneralOpts,
			},
			correct:     false,
			errMsgRegex: regexp.MustCompile("^connection failed\n  cause.*"),
		},
		{
			name:        "bulk insert failed",
			options:     defaultOpts("testdata/invalid-content.json"),
			correct:     false,
			errMsgRegex: regexp.MustCompile("^exception occurred during bulk insert.*\n  cause.*\n Try.*"),
		},
		{
			name:        "wrong shardconfig",
			options:     defaultOpts("testdata/invalid-shardconfig.json"),
			errMsgRegex: regexp.MustCompile("^wrong value for 'shardConfig.shardCollection'.*"),
			correct:     false,
		},
		{
			name:        "fail to shard on single mongod",
			options:     defaultOpts("testdata/invalid-shardconfig-singlemongod.json"),
			errMsgRegex: regexp.MustCompile("^couldn't create sharded collection.*"),
			correct:     false,
		},
		{
			name:        "fail to shard with non _id key",
			options:     defaultOpts("testdata/invalid-shardconfig-nonid.json"),
			errMsgRegex: regexp.MustCompile("^couldn't create sharded collection.*"),
			correct:     false,
		},
		{
			name:        "empty shard key",
			options:     defaultOpts("testdata/invalid-shardconfig-emptykey.json"),
			errMsgRegex: regexp.MustCompile("^wrong value for 'shardConfig.key'.*"),
			correct:     false,
		},
	}

	var r struct {
		N int `bson:"n"`
	}

	for _, tt := range realRunTests {
		t.Run(tt.name, func(t *testing.T) {
			err := datagen.Generate(&tt.options, os.Stdout)
			if tt.correct {
				if err != nil {
					t.Errorf("expected no error for options %v, but got %v", tt.options, err)
				}
				if tt.expectedNbDoc > 0 {
					db := session.DB("datagen_it_test")
					command := bson.D{
						{Name: "count", Value: db.C("test_bson").Name},
					}
					err = db.Run(command, &r)
					if err != nil {
						t.Errorf("fail to run count command: %v", err)
					}
					if r.N != tt.expectedNbDoc {
						t.Errorf("expected %d docs but got %d", tt.expectedNbDoc, r.N)
					}
				}
			} else {
				if err == nil {
					t.Errorf("expected an error for options %v", tt.options)
				}
				if !tt.errMsgRegex.MatchString(err.Error()) {
					t.Errorf("error msg should match regex %v, but was %v", tt.errMsgRegex.String(), err)
				}
			}
		})
	}
}

func defaultOpts(configFile string) datagen.Options {
	return datagen.Options{
		Connection: defaultConnOpts,
		General:    defaultGeneralOpts,
		Configuration: datagen.Configuration{
			ConfigFile: configFile,
			BatchSize:  1000,
		},
	}
}

type distinctResult struct {
	Values []interface{} `bson:"values"`
}

func distinct(t *testing.T, dbName, collName, keyName string, result distinctResult) int {
	err := session.DB(dbName).Run(bson.D{
		{Name: "distinct", Value: collName},
		{Name: "key", Value: keyName},
	}, &result)
	if err != nil {
		t.Error(err)
	}
	return len(result.Values)
}

func parseConfig(t *testing.T, fileName string) []datagen.Collection {
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		t.Error(err)
	}
	c, err := datagen.ParseConfig(content, false)
	if err != nil {
		t.Error(err)
	}
	return c
}
