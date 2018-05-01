// Package datagen used to generate millions of pseudo-random BSON documents
// and insert them into a Mongodb instance.
package datagen

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"gopkg.in/cheggaaa/pb.v2"

	"github.com/feliixx/mgodatagen/datagen/generators"
)

type result struct {
	Ok     bool
	ErrMsg string
	Shards []bson.M
}

// get a connection from Connection args
func connectToDB(conn *Connection, out io.Writer) (*mgo.Session, []int, error) {
	fmt.Fprintf(out, "Connecting to mongodb://%s:%s\n", conn.Host, conn.Port)
	url := "mongodb://"
	if conn.UserName != "" && conn.Password != "" {
		url += conn.UserName + ":" + conn.Password + "@"
	}
	session, err := mgo.DialWithTimeout(url+conn.Host+":"+conn.Port, conn.Timeout)
	if err != nil {
		return nil, nil, fmt.Errorf("connection failed\n  cause: %v", err)
	}
	infos, _ := session.BuildInfo()
	fmt.Fprintf(out, "mongodb server version %s\n\n", infos.Version)

	version := strings.Split(infos.Version, ".")
	versionInt := make([]int, len(version))

	for i := range version {
		v, _ := strconv.Atoi(version[i])
		versionInt[i] = v
	}

	var r result
	// if it's a sharded cluster, print the list of shards. Don't bother with the error
	// if cluster is not sharded / user not allowed to run command against admin db
	err = session.Run(bson.M{"listShards": 1}, &r)
	if err == nil && r.ErrMsg == "" {
		json, err := json.MarshalIndent(r.Shards, "", "  ")
		if err == nil {
			fmt.Fprintf(out, "shard list: %v\n", string(json))
		}
	}
	return session, versionInt, nil
}

type dtg struct {
	out     io.Writer
	session *mgo.Session
	version []int
	Options
}

func (d *dtg) generate(v *Collection) error {
	err := d.createCollection(v)
	if err != nil {
		return err
	}
	if !d.IndexOnly {
		err = d.fillCollection(v)
		if err != nil {
			return err
		}
	}
	err = d.ensureIndex(v)
	if err != nil {
		return err
	}
	return d.printCollStats(v)
}

// create a collection with specific options
func (d *dtg) createCollection(coll *Collection) error {
	c := d.session.DB(coll.DB).C(coll.Name)

	if d.Append || d.IndexOnly {
		return nil
	}

	c.DropCollection()
	fmt.Fprintf(d.out, "Creating collection %s...\n", coll.Name)

	if coll.CompressionLevel != "" {
		err := c.Create(&mgo.CollectionInfo{StorageEngine: bson.M{"wiredTiger": bson.M{"configString": "block_compressor=" + coll.CompressionLevel}}})
		if err != nil {
			return fmt.Errorf("coulnd't create collection with compression level %s:\n  cause: %v", coll.CompressionLevel, err)
		}
	}
	if coll.ShardConfig.ShardCollection != "" {
		nm := c.Database.Name + "." + c.Name
		if coll.ShardConfig.ShardCollection != nm {
			return fmt.Errorf("wrong value for 'shardConfig.shardCollection', should be <database>.<collection>: found %s, expected %s", coll.ShardConfig.ShardCollection, nm)
		}
		if len(coll.ShardConfig.Key) == 0 {
			return fmt.Errorf("wrong value for 'shardConfig.key', can't be null and must be an object like {'_id': 'hashed'}, found: %v", coll.ShardConfig.Key)
		}
		var r result
		// index to shard the collection
		// if shard key is '_id', no need to rebuild the index
		if coll.ShardConfig.Key["_id"] == 1 {
			index := Index{
				Name: "shardKey",
				Key:  coll.ShardConfig.Key,
			}
			err := c.Database.Run(bson.D{
				{Name: "createIndexes", Value: c.Name},
				{Name: "indexes", Value: [1]Index{index}},
			}, &r)
			if err != nil || !r.Ok {
				return handleCommandError(fmt.Sprintf("couldn't create shard key with index config %v", index.Key), err, &r)
			}
		}
		err := d.session.Run(coll.ShardConfig, &r)
		if err != nil || !r.Ok {
			return handleCommandError("couldn't create sharded collection. Make sure that sharding is enabled,\n see https://docs.mongodb.com/manual/reference/command/enableSharding/#dbcmd.enableSharding for details", err, &r)
		}
	}
	return nil
}

type rawChunk struct {
	documents  []bson.Raw
	nbToInsert int
}

// use a sync.Pool to reduce memory consumption
// also reduce the nb of items to send to the channel
var pool = sync.Pool{
	New: func() interface{} {
		list := make([]bson.Raw, 1000)
		for i := range list {
			list[i] = bson.Raw{
				Data: make([]byte, 0),
				Kind: bson.ElementDocument,
			}
		}
		return &rawChunk{
			documents: list,
		}
	},
}

func (d *dtg) fillCollection(coll *Collection) error {

	seed := uint64(time.Now().Unix())
	ci := generators.NewCollInfo(coll.Count, d.ShortName, d.version, seed)

	docGenerator, err := ci.DocumentGenerator(coll.Content)
	if err != nil {
		return err
	}
	nbInsertingGoRoutines := runtime.NumCPU()
	if d.NumInsertWorker > 0 {
		nbInsertingGoRoutines = d.NumInsertWorker
	}
	taskBufferSize := 10
	// for really small insert, use only one goroutine and reduce the buffered channel size
	if coll.Count <= 10000 {
		nbInsertingGoRoutines = 1
		taskBufferSize = 1
	}

	task := make(chan *rawChunk, taskBufferSize)
	errs := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(nbInsertingGoRoutines)

	bar := pb.ProgressBarTemplate(`{{counters .}} {{ bar . "[" "=" ">" " " "]"}} {{percent . }}   {{speed . "%s doc/s" }}   {{rtime . "%s"}}          `).Start(int(coll.Count))
	bar.SetWriter(d.out)

	for i := 0; i < nbInsertingGoRoutines; i++ {
		go func() {
			defer wg.Done()
			//use session.Copy() so each connection use a distinct socket
			s := d.session.Copy()
			defer s.Close()
			c := s.DB(coll.DB).C(coll.Name)

			for t := range task {
				// if an error occurs in one of the goroutine, 'return' is called which trigger
				// wg.Done() ==> the goroutine stops
				select {
				case <-ctx.Done():
					return
				default:
				}
				bulk := c.Bulk()
				bulk.Unordered()

				for i := 0; i < t.nbToInsert; i++ {
					bulk.Insert(t.documents[i])
				}
				_, err := bulk.Run()
				if err != nil {
					// if the bulk insert fails, push the error to the error channel
					// so that we can use it from the main thread
					select {
					case errs <- fmt.Errorf("exception occurred during bulk insert:\n  cause: %v\n Try to set a smaller batch size with -b | --batchsize option", err):
					default:
					}
					// cancel the context to terminate goroutine and stop the feeding of the
					// buffered channel
					cancel()
					return
				}
				// return the rawchunk to the pool so it can be reused
				pool.Put(t)
			}
		}()
	}
	count := 0
	// start bson.Raw generation to feed the task channel
Loop:
	for count < coll.Count {
		select {
		case <-ctx.Done(): // if an error occurred in one of the 'inserting' goroutines, close the channel
			break Loop
		default:
		}
		rc := pool.Get().(*rawChunk)
		rc.nbToInsert = d.BatchSize
		if coll.Count-count < d.BatchSize {
			rc.nbToInsert = int(coll.Count - count)
		}
		for i := 0; i < rc.nbToInsert; i++ {
			docGenerator.Value()
			l := ci.Encoder.Len()
			// if documents[i] is not large enough, grow it manually
			if len(rc.documents[i].Data) < l {
				for j := len(rc.documents[i].Data); j < l; j++ {
					rc.documents[i].Data = append(rc.documents[i].Data, byte(0))
				}
			} else {
				rc.documents[i].Data = rc.documents[i].Data[:l]
			}
			copy(rc.documents[i].Data, ci.Encoder.Bytes())
		}
		count += rc.nbToInsert
		bar.Add(rc.nbToInsert)
		task <- rc
	}
	close(task)

	wg.Wait()
	bar.Finish()

	if ctx.Err() != nil {
		return <-errs
	}
	err = d.updateWithAggregators(coll)
	if err != nil {
		return err
	}
	color.New(color.FgGreen).Fprintf(d.out, "Generating collection %s done\n", coll.Name)
	return nil
}

// Update documents with pre-computed aggregations
func (d *dtg) updateWithAggregators(coll *Collection) error {

	ci := generators.NewCollInfo(coll.Count, d.ShortName, d.version, 0)
	aggregators, err := ci.DocumentAggregator(coll.Content)
	if err != nil {
		return err
	}
	if len(aggregators) == 0 {
		return nil
	}
	fmt.Fprintf(d.out, "Generating aggregated data for collection %v\n", coll.Name)
	bar := pb.ProgressBarTemplate(`{{counters .}} {{ bar . "[" "=" ">" " " "]"}} {{percent . }}                          `).Start(int(coll.Count))
	bar.SetWriter(d.out)
	defer bar.Finish()
	// aggregation might be very long, so make sure the connection won't timeout
	d.session.SetSocketTimeout(time.Duration(30) * time.Minute)

	tasks := make(chan [2]bson.M, d.BatchSize)
	errs := make(chan error)

	// run updates in a new goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s := d.session.Copy()
		defer s.Close()
		coll := s.DB(coll.DB).C(coll.Name)
		bulk := coll.Bulk()
		bulk.Unordered()
		count := 0
		for t := range tasks {
			count++
			bulk.Update(t[0], t[1])
			if count%d.BatchSize == 0 {
				_, err := bulk.Run()
				if err != nil {
					errs <- fmt.Errorf("exception occurred during bulk insert:\n  cause: %v\n Try to set a smaller batch size with -b | --batchsize option", err)
					return
				}
				bulk := coll.Bulk()
				bulk.Unordered()
				count = 0
			}
		}
		if count > 0 {
			_, err := bulk.Run()
			if err != nil {
				errs <- fmt.Errorf("exception occurred during bulk insert:\n  cause: %v\n Try to set a smaller batch size with -b | --batchsize option", err)
			}
		}
	}()

	c := d.session.DB(coll.DB).C(coll.Name)
	var aggregationError error

Loop:
	for _, aggregator := range aggregators {

		localVar := aggregator.LocalVar()
		var result struct {
			Values []interface{} `bson:"values"`
		}
		err = c.Database.Run(bson.D{
			{Name: "distinct", Value: c.Name},
			{Name: "key", Value: localVar},
		}, &result)
		if err != nil {
			aggregationError = fmt.Errorf("fail to get distinct values for local field %v: %v", localVar, err)
			break Loop
		}
		for _, value := range result.Values {
			select {
			case err := <-errs:
				aggregationError = err
				break Loop
			default:
			}

			update, err := aggregator.Update(d.session, value)
			if err != nil {
				aggregationError = err
				break Loop
			}
			tasks <- update
		}
		bar.Add(int(coll.Count) / len(aggregators))
	}
	close(tasks)
	wg.Wait()
	return aggregationError
}

// create index on generated collections
func (d *dtg) ensureIndex(coll *Collection) error {
	if len(coll.Indexes) == 0 {
		fmt.Fprintf(d.out, "No index to build for collection %s\n\n", coll.Name)
		return nil
	}
	fmt.Fprintf(d.out, "Building indexes for collection %s...\n", coll.Name)

	c := d.session.DB(coll.DB).C(coll.Name)
	err := c.DropAllIndexes()
	if err != nil {
		return fmt.Errorf("error while dropping index for collection %s:\n  cause: %v", coll.Name, err)
	}
	// avoid timeout when building indexes
	d.session.SetSocketTimeout(time.Duration(30) * time.Minute)

	var r result
	err = c.Database.Run(bson.D{
		{Name: "createIndexes", Value: c.Name},
		{Name: "indexes", Value: coll.Indexes},
	}, &r)
	if err != nil || !r.Ok {
		return handleCommandError(fmt.Sprintf("error while building indexes for collection %s", coll.Name), err, &r)
	}
	color.New(color.FgGreen).Fprintf(d.out, "Building indexes for collection %s done\n\n", coll.Name)
	return nil
}

func (d *dtg) printCollStats(coll *Collection) error {
	c := d.session.DB(coll.DB).C(coll.Name)
	stats := struct {
		Count      int64  `bson:"count"`
		AvgObjSize int64  `bson:"avgObjSize"`
		IndexSizes bson.M `bson:"indexSizes"`
	}{}
	err := c.Database.Run(bson.D{
		{Name: "collStats", Value: c.Name},
		{Name: "scale", Value: 1024},
	}, &stats)
	if err != nil {
		return fmt.Errorf("couldn't get stats for collection %s \n  cause: %v ", c.Name, err)
	}
	indexString := ""
	for k, v := range stats.IndexSizes {
		indexString += fmt.Sprintf("%s %v KB\n\t\t    ", k, v)
	}
	fmt.Fprintf(d.out, "Stats for collection %s:\n\t - doc count: %v\n\t - average object size: %v bytes\n\t - indexes: %s\n", c.Name, stats.Count, stats.AvgObjSize, indexString)
	return nil
}

func createEmptyCfgFile(filename string) error {
	_, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		fmt.Printf("file %s already exists, overwrite it ?  [y/n]: ", filename)
		response := make([]byte, 2)
		_, err := os.Stdin.Read(response)
		if err != nil {
			return fmt.Errorf("couldn't read from user, aborting")
		}
		if string(response[0]) != "y" {
			return nil
		}
	}
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	template := `[{
"database": "dbName",
"collection": "collectionName",
"count": 1000,
"content": {
    
  }
}]
`
	_, err = f.Write([]byte(template))
	return err
}

func handleCommandError(msg string, err error, r *result) error {
	m := err.Error()
	if !r.Ok {
		m = r.ErrMsg
	}
	return fmt.Errorf("%s\n  cause: %s", msg, m)
}

// General struct that stores global options from command line args
type General struct {
	Help    bool `long:"help" description:"show this help message"`
	Version bool `short:"v" long:"version" description:"print the tool version and exit"`
	Quiet   bool `short:"q" long:"quiet" description:"quieter output"`
}

// Connection struct that stores info on connection from command line args
type Connection struct {
	Host     string `short:"h" long:"host" value-name:"<hostname>" description:"mongodb host to connect to" default:"127.0.0.1"`
	Port     string `long:"port" value-name:"<port>" description:"server port" default:"27017"`
	UserName string `short:"u" long:"username" value-name:"<username>" description:"username for authentification"`
	Password string `short:"p" long:"password" value-name:"<password>" description:"password for authentification"`
	Timeout  time.Duration
}

// Configuration struct that stores info on config file from command line args
type Configuration struct {
	ConfigFile      string `short:"f" long:"file" value-name:"<configfile>" description:"JSON config file. This field is required"`
	IndexOnly       bool   `short:"i" long:"indexonly" description:"if present, mgodatagen will just try to rebuild index"`
	ShortName       bool   `short:"s" long:"shortname" description:"if present, JSON keys in the documents will be reduced\n to the first two letters only ('name' => 'na')"`
	Append          bool   `short:"a" long:"append" description:"if present, append documents to the collection without\n removing older documents or deleting the collection"`
	NumInsertWorker int    `short:"n" long:"numWorker" value-name:"<nb>" description:"number of concurrent workers inserting documents\n in database. Default is number of CPU+1"`
	BatchSize       int    `short:"b" long:"batchsize" value-name:"<size>" description:"bulk insert batch size" default:"1000"`
}

// Template struct that stores info on config file to generate
type Template struct {
	New string `long:"new" value-name:"<filename>" description:"create an empty configuration file"`
}

// Options struct to store flags from CLI
type Options struct {
	Template      `group:"template"`
	Configuration `group:"configuration"`
	Connection    `group:"connection infos"`
	General       `group:"general"`
}

const (
	mgodatagenVersion = "0.7.0"
	defaultTimeout    = 10 * time.Second
)

// Generate create a database according to specified options. Progress informations
// are send to out
func Generate(options *Options, out io.Writer) error {
	return run(options, out)
}

func run(options *Options, out io.Writer) error {
	if options.Quiet {
		out = ioutil.Discard
	}
	if options.Version {
		fmt.Fprintf(out, "mgodatagen version %s\n", mgodatagenVersion)
		return nil
	}
	if options.New != "" {
		err := createEmptyCfgFile(options.New)
		if err != nil {
			return fmt.Errorf("could not create an empty configuration file: %v", err)
		}
		return nil
	}
	if options.ConfigFile == "" {
		return fmt.Errorf("No configuration file provided, try mgodatagen --help for more informations ")
	}
	if options.BatchSize > 1000 || options.BatchSize <= 0 {
		return fmt.Errorf("invalid value for -b | --batchsize: %v. BatchSize has to be between 1 and 1000", options.BatchSize)
	}
	fmt.Fprintln(out, "Parsing configuration file...")
	content, err := ioutil.ReadFile(options.ConfigFile)
	if err != nil {
		return fmt.Errorf("File error: %v", err)
	}
	collectionList, err := ParseConfig(content, false)
	if err != nil {
		return err
	}
	if options.Connection.Timeout == 0 {
		options.Connection.Timeout = defaultTimeout
	}
	session, version, err := connectToDB(&options.Connection, out)
	if err != nil {
		return err
	}
	defer session.Close()

	dtg := &dtg{
		out:     out,
		session: session,
		version: version,
		Options: *options,
	}

	for _, v := range collectionList {
		err = dtg.generate(&v)
		if err != nil {
			return err
		}
	}
	return nil
}
