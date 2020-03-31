package datagen_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"go.mongodb.org/mongo-driver/mongo/options"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/feliixx/mgodatagen/datagen"
)

var (
	session         *mongo.Client
	defaultConnOpts = datagen.Connection{
		Host: "127.0.0.1",
		Port: "27017",
	}
	defaultGeneralOpts = datagen.General{
		Quiet: true,
	}
)

func TestMain(m *testing.M) {

	s, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
	if err != nil {
		fmt.Printf("fail to connect to mongodb: %v", err)
		os.Exit(1)
	}
	session = s
	defer s.Disconnect(context.Background())

	retCode := m.Run()

	err = session.Database("datagen_it_test").Drop(context.Background())
	if err != nil {
		fmt.Printf("couldn't drop db: %v\n", err)
		os.Exit(1)
	}
	os.Exit(retCode)
}

func TestCreateEmptyFile(t *testing.T) {

	filename := "testNewFile.json"

	opts := &datagen.Options{
		Template: datagen.Template{
			New: filename,
		},
	}
	err := datagen.Generate(opts, ioutil.Discard)
	if err != nil {
		t.Errorf("expected no error for creating empty file but got %v", err)
	}
	defer os.Remove(filename)

	testNewFileContent(t, filename)
}

func TestCreateEmptyFileOverwrite(t *testing.T) {

	filename := "testFileAlreadyExist.json"

	_, err := os.Create(filename)
	if err != nil {
		t.Errorf("fail to create file: %v", err)
	}
	defer os.Remove(filename)

	fakeUsrInput, err := ioutil.TempFile("", "fake_user_input")
	if err != nil {
		t.Error(err)
	}
	defer os.Remove(fakeUsrInput.Name())

	if _, err := fakeUsrInput.Write([]byte("y")); err != nil {
		log.Fatal(err)
	}
	if _, err := fakeUsrInput.Seek(0, 0); err != nil {
		log.Fatal(err)
	}

	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()

	os.Stdin = fakeUsrInput

	options := &datagen.Options{
		Template: datagen.Template{
			New: filename,
		},
	}
	err = datagen.Generate(options, ioutil.Discard)
	if err != nil {
		t.Errorf("expected no error for creating empty file but got %v", err)
	}
	testNewFileContent(t, filename)
}

func testNewFileContent(t *testing.T, filename string) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Error(err)
	}
	var collections []datagen.Collection
	err = json.Unmarshal(content, &collections)
	if err != nil {
		t.Errorf("invalid template file: %v", err)
	}
	if len(collections) == 0 {
		t.Errorf("expected at least one collection but got none")
	}
}

func TestProgressOutput(t *testing.T) {

	configFile := "testdata/empty.json"
	opts := defaultOpts(configFile)
	opts.Quiet = false

	var buffer bytes.Buffer
	err := datagen.Generate(&opts, &buffer)
	if err != nil {
		t.Error(err)
	}

	b, err := ioutil.ReadFile("testdata/output.txt")
	if err != nil {
		t.Error(err)
	}
	// only compare content of the stat table
	// TODO find a cleaner way to do this
	expected := bytes.SplitN(b, []byte("+"), 2)
	output := bytes.SplitN(buffer.Bytes(), []byte("+"), 2)

	want, got := expected[1][:45], output[1][:45]
	if !bytes.Equal(want, got) {
		t.Errorf("expected \n%s \n but got \n%s", want, got)
	}
}

func TestCollectionContent(t *testing.T) {

	configFile := "generators/testdata/full-bson.json"
	collections := parseConfig(t, configFile)

	opts := defaultOpts(configFile)
	err := datagen.Generate(&opts, ioutil.Discard)
	if err != nil {
		t.Error(err)
	}

	c := session.Database(collections[0].DB).Collection(collections[0].Name)
	docCount, err := c.CountDocuments(context.Background(), bson.M{})
	if err != nil {
		t.Error(err)
	}
	if want, got := int64(collections[0].Count), docCount; want != got {
		t.Errorf("expected %d documents but got %d", want, got)
	}

	var results []struct {
		ID                      primitive.ObjectID `bson:"_id"`
		UUID                    string             `bson:"uuid"`
		String                  string             `bson:"string"`
		Int32                   int32              `bson:"int32"`
		Int64                   int64              `bson:"int64"`
		Float                   float64            `bson:"float"`
		ConstInt32              int32              `bson:"constInt32"`
		ConstInt64              int64              `bson:"constInt64"`
		ConstFloat              float64            `bson:"constFloat"`
		Boolean                 bool               `bson:"boolean"`
		Position                []float64          `bson:"position"`
		StringFromArray         string             `bson:"stringFromArray"`
		IntFromArrayRandomOrder int                `bson:"intFromArrayRandomOrder"`
		ArrayFromArray          []string           `bson:"arrayFromArray"`
		ConstArray              []string           `bson:"constArray"`
		Fake                    string             `bson:"faker"`
		Constant                int32              `bson:"constant"`
		AutoIncrementInt32      int32              `bson:"autoIncrementInt32"`
		AutoIncrementInt64      int64              `bson:"autoIncrementInt64"`
		Date                    time.Time          `bson:"date"`
		BinaryData              []byte             `bson:"binaryData"`
		ArrayInt32              []int32            `bson:"arrayInt32"`
		Object                  struct {
			K1    string `bson:"k1"`
			K2    int32  `bson:"k2"`
			Subob struct {
				Sk int32 `bson:"s-k"`
			} `bson:"sub-ob"`
		} `bson:"object"`
	}
	cursor, err := c.Find(context.Background(), bson.M{})
	if err != nil {
		t.Error(err)
	}
	err = cursor.All(context.Background(), &results)
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

		if len(r.UUID) != 36 {
			t.Errorf("len(uuid) should be 16, but was %d", len(r.UUID))
		}

		if len(r.String) < 15 || len(r.String) > 20 {
			t.Errorf("expected 15 < len(String) < 20, but was %d", len(r.String))
		}
		if r.Int32 < 10 || r.Int32 > 20 {
			t.Errorf("expected 10 < Int32 < 20, but was %d", r.Int32)
		}
		if r.ConstInt32 != 0 {
			t.Errorf("ConstInt32 should always be 0, but was %d", r.ConstInt32)
		}
		if r.Int64 == 0 {
			count++
		} else {
			if r.Int64 < 100 || r.Int64 > 200 {
				t.Errorf("expected 100 < Int64 < 200, but was %d", r.Int64)
			}
		}
		if r.ConstInt64 != -100020 {
			t.Errorf("ConstInt64 should always be -100020, but was %d", r.ConstInt64)
		}
		if r.Float < 0 || r.Float > 10 {
			t.Errorf("expected 0 < Float < 10, but was %f", r.Float)
		}
		if r.ConstFloat != 0.0 {
			t.Errorf("ConstFloat should always be 0.0, but was %f", r.ConstFloat)
		}
		if r.Position[0] < -90 || r.Position[0] > 90 {
			t.Errorf("expected -90 < pos[0] < 90, but was %f", r.Position[0])
		}
		if r.Position[1] < -180 || r.Position[1] > 180 {
			t.Errorf("expected -180 < pos[1] < 180, but was %f", r.Position[1])
		}
		idx := fromArr.Search(r.StringFromArray)
		if idx == len(fromArr) || fromArr[idx] != r.StringFromArray {
			t.Errorf("StringFromArray should be in %v, but was %s", r.StringFromArray, fromArr)
		}
		if r.Constant != int32(2) {
			t.Errorf("'Constant' field should be int32(2), but was %v", r.Constant)
		}
		if r.AutoIncrementInt32 != int32(i) {
			t.Errorf("'AutoIncrementInt32' field should be %d, but was %d", int32(i), r.AutoIncrementInt32)
		}
		if r.AutoIncrementInt64 != int64(i+18) {
			t.Errorf("'AutoIncrementInt64' field should be %d, but was %d", int64(i+18), r.AutoIncrementInt64)
		}

		dt := expectedDate.Sub(r.Date)
		delta := time.Second * 60 * 60 * 24 * 365 * 4
		if dt < -delta || dt > delta {
			t.Errorf("'date' should be within %v, but was %v", delta, r.Date)
		}
		if len(r.BinaryData) < 24 || len(r.BinaryData) > 40 {
			t.Errorf("expected 4 < len(BinaryData) < 40, but was %d", len(r.BinaryData))
		}
		if len(r.ArrayInt32) != 3 {
			t.Errorf("'ArrayInt32' should have 3 items but got only %d", len(r.ArrayInt32))
		}
		if len(r.ArrayFromArray) != 6 {
			t.Errorf("'ArrayFromArray' should have 6 items but got only %d", len(r.ArrayFromArray))
		}
		if len(r.ConstArray) != 2 {
			t.Errorf("'ConstArray' should have 2 items but got only %d", len(r.ConstArray))
		}
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
		t.Errorf("doc nb with int64 == null should be 75 < count < 125 (2.5percent) but was %d", count)
	}

	// we expect fixed values for those keys
	maxDistinctValuesTests := map[string]int{
		// test maxDistinctValues option
		"name": int(collections[0].Content["name"].MaxDistinctValue),
		// test unique option
		"object.k1": 1000,
		// test value distribution
		"stringFromArray":         4,
		"intFromArrayRandomOrder": 7,
		"_id":                     1000,
		"int32":                   11,
		"arrayInt32":              11,
		"autoIncrementInt32":      1000,
		"autoIncrementInt64":      1000,
		"boolean":                 2,
		"float":                   1000,
		"position":                2000,
	}
	for key, value := range maxDistinctValuesTests {
		nb := nbDistinctValue(t, collections[0].DB, collections[0].Name, key)
		if value != nb {
			t.Errorf("for field %s, expected %d distinct values but got %d", key, value, nb)
		}
	}
	// nbDistinctValue count may be different from one run to another due
	// to nullPercentage != 0
	maxDistinctValuesNullPercentageTests := map[string]int{
		"int64": 80,
	}

	for key, value := range maxDistinctValuesNullPercentageTests {
		nb := nbDistinctValue(t, collections[0].DB, collections[0].Name, key)
		if value > nb {
			t.Errorf("for field %s, expected %d max distinct values but got %d", key, value, nb)
		}
	}
}

func TestCollectionWithRef(t *testing.T) {

	configFile := "generators/testdata/ref.json"
	collections := parseConfig(t, configFile)
	opts := defaultOpts(configFile)
	err := datagen.Generate(&opts, ioutil.Discard)
	if err != nil {
		t.Error(err)
	}

	var distinct struct {
		Values []primitive.ObjectID
	}
	result := session.Database(collections[0].DB).RunCommand(context.Background(), bson.D{
		bson.E{Key: "distinct", Value: collections[0].Name},
		bson.E{Key: "key", Value: "_id"},
	})
	if err := result.Err(); err != nil {
		t.Error(err)
	}
	if err := result.Decode(&distinct); err != nil {
		t.Error(err)
	}

	c := session.Database(collections[1].DB).Collection(collections[1].Name)
	var refResult []struct {
		ID  primitive.ObjectID
		Ref primitive.ObjectID
	}
	cursor, err := c.Find(context.Background(), bson.M{}, options.Find().SetSort(bson.M{"_id": 1}))
	if err != nil {
		t.Error(err)
	}
	err = cursor.All(context.Background(), &refResult)
	if err != nil {
		t.Error(err)
	}

	for _, r := range distinct.Values {
		i := sort.Search(len(refResult), func(i int) bool { return refResult[i].Ref.String() >= r.String() })
		if i == len(refResult) || refResult[i].Ref.String() != r.String() {
			t.Errorf("%v not found in result", r.String())
		}
	}
}

func TestCollectionContentWithAggregation(t *testing.T) {

	configFile := "generators/testdata/full-aggregation.json"
	collections := parseConfig(t, configFile)
	opts := defaultOpts(configFile)
	err := datagen.Generate(&opts, ioutil.Discard)
	if err != nil {
		t.Error(err)
	}

	c := session.Database(collections[1].DB).Collection(collections[1].Name)
	var results []bson.M
	cursor, err := c.Find(context.Background(), bson.M{})
	if err != nil {
		t.Error(err)
	}
	err = cursor.All(context.Background(), &results)
	if err != nil {
		t.Error(err)
	}

	possibleValues := sort.StringSlice{"a", "b", "c", "d", "e", "f", "g", "h", "i"}

	for _, r := range results {
		b := r["AG-CI"].(bson.M)
		m := b["m"].(int32)
		if m < 0 || m > 100 {
			t.Errorf("'m' field should be 0 < m < 100, but was %d", m)
		}
		mM := b["M"].(int32)
		if mM < 9900 || mM > 10000 {
			t.Errorf("'M' field should be 900 < M < 1000, but was %d", mM)
		}
		agFI := r["AG-FI"].(int64)
		if agFI < 1450 || agFI > 1850 {
			t.Errorf("'AG-FI' field should be 1450 < AG-FI < 1850, but was %d", agFI)
		}

		vv := r["AG-VA"].(primitive.A)
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

	var stats struct {
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
				result := session.Database(collections[0].DB).RunCommand(context.Background(), bson.D{
					bson.E{Key: "collStats", Value: collections[0].Name},
				})
				if err := result.Err(); err != nil {
					t.Error(err)
				}
				if err := result.Decode(&stats); err != nil {
					t.Error(err)
				}
				if !strings.Contains(stats.WiredTiger.CreationString, "block_compressor="+tt.compressionLevel) {
					t.Errorf("block_compressor should be %s, but result was %s", tt.compressionLevel, stats.WiredTiger.CreationString)
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
			errMsgRegex: regexp.MustCompile("^error while building indexes for collection.*\n cause.*"),
		},
	}

	for _, tt := range createCollectionWithIndexesTests {
		t.Run(tt.name, func(t *testing.T) {
			collections := parseConfig(t, tt.configFile)
			opts := defaultOpts(tt.configFile)
			err := datagen.Generate(&opts, ioutil.Discard)
			if tt.correct {
				if err != nil {
					t.Errorf("ensureIndex with indexes %v should not fail: \n%v", tt.indexes, err)
				}
				cursor, err := session.Database(collections[0].DB).Collection(collections[0].Name).Indexes().List(context.Background())
				if err != nil {
					t.Error(err)
				}

				var idx struct {
					Name string
				}

				i := 0
				for cursor.Next(context.Background()) {

					if err := cursor.Decode(&idx); err != nil {
						t.Error(err)
					}
					// index on "_id" is created by default
					if idx.Name == "_id_" {
						continue
					}

					if want, got := tt.indexes[i].Name, idx.Name; want != got {
						t.Errorf("index does not match: expected %s, got %s", want, got)
					}
					i++
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
		expectedNbDoc int64
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

	for _, tt := range realRunTests {
		t.Run(tt.name, func(t *testing.T) {
			err := datagen.Generate(&tt.options, os.Stdout)
			if tt.correct {
				if err != nil {
					t.Errorf("expected no error for options %v, but got %v", tt.options, err)
				}
				if tt.expectedNbDoc > 0 {
					count, err := session.Database("datagen_it_test").Collection("test_bson").CountDocuments(context.Background(), bson.M{})
					if err != nil {
						t.Error(err)
					}
					if count != tt.expectedNbDoc {
						t.Errorf("expected %d docs but got %d", tt.expectedNbDoc, count)
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

func nbDistinctValue(t *testing.T, dbName, collName, keyName string) int {

	var distinct struct {
		Values []interface{}
	}

	result := session.Database(dbName).RunCommand(context.Background(), bson.D{
		bson.E{Key: "distinct", Value: collName},
		bson.E{Key: "key", Value: keyName},
	})
	if err := result.Err(); err != nil {
		t.Error(err)
	}
	if err := result.Decode(&distinct); err != nil {
		t.Error(err)
	}
	return len(distinct.Values)
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

func BenchmarkGenerate(b *testing.B) {

	opts := defaultOpts("testdata/big.json")
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {

		b.StopTimer()
		err := session.Database("datagen_it_test").Drop(context.Background())
		if err != nil {
			b.Error(err)
		}
		b.StartTimer()

		datagen.Generate(&opts, ioutil.Discard)
	}

}
