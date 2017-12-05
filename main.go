// A small CLI tool to quickly generate millions of pseudo-random BSON documents
// and insert them into a Mongodb instance.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/jessevdk/go-flags"
	"gopkg.in/cheggaaa/pb.v2"

	"github.com/feliixx/mgodatagen/config"
	"github.com/feliixx/mgodatagen/generators"
)

const (
	version = "0.4.2" // current version of mgodatagen
)

// get a connection from Connection args
func connectToDB(conn *Connection) (*mgo.Session, []int, error) {
	fmt.Printf("Connecting to mongodb://%s:%s\n\n", conn.Host, conn.Port)
	url := "mongodb://"
	if conn.UserName != "" && conn.Password != "" {
		url += conn.UserName + ":" + conn.Password + "@"
	}
	session, err := mgo.Dial(url + conn.Host + ":" + conn.Port)
	if err != nil {
		return nil, nil, fmt.Errorf("connection failed:\n\tcause: %s", err.Error())
	}
	infos, err := session.BuildInfo()
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't get mongodb version:\n\tcause: %s", err.Error())
	}
	fmt.Printf("mongodb server version %s\ngit version %s\nOpenSSL version %s\n\n", infos.Version, infos.GitVersion, infos.OpenSSLVersion)
	version := strings.Split(infos.Version, ".")
	versionInt := make([]int, len(version))

	for i := range version {
		v, _ := strconv.Atoi(version[i])
		versionInt[i] = v
	}

	result := struct {
		ErrMsg string
		Shards []bson.M
	}{}
	// if it's a sharded cluster, print the list of shards. Don't bother with the error
	// if cluster is not sharded / user not allowed to run command against admin db
	err = session.Run(bson.M{"listShards": 1}, &result)
	if err == nil && result.ErrMsg == "" {
		json, err := json.MarshalIndent(result.Shards, "", "  ")
		if err == nil {
			fmt.Printf("shard list: %v\n", string(json))
		}
	}
	return session, versionInt, nil
}

// create a collection with specific options
func (d *datagen) createCollection(coll *config.Collection) error {
	c := d.session.DB(coll.DB).C(coll.Name)
	// if indexOnly or append mode, just return the collection as it already exists
	if d.Append || d.IndexOnly {
		return nil
	}
	// drop the collection before inserting new document. Ignore the error
	// if the collection does not exists
	c.DropCollection()
	fmt.Fprintf(d.out, "Creating collection %s...\n", coll.Name)

	if coll.CompressionLevel != "" {
		err := c.Create(&mgo.CollectionInfo{StorageEngine: bson.M{"wiredTiger": bson.M{"configString": "block_compressor=" + coll.CompressionLevel}}})
		if err != nil {
			return fmt.Errorf("coulnd't create collection with compression level %s:\n\tcause: %s", coll.CompressionLevel, err.Error())
		}
	}
	if coll.ShardConfig.ShardCollection != "" {
		result := struct {
			ErrMsg string
			Ok     bool
		}{}
		// check that the config is correct
		nm := c.Database.Name + "." + c.Name
		if coll.ShardConfig.ShardCollection != nm {
			return fmt.Errorf("wrong value for 'shardConfig.shardCollection', should be <database>.<collection>: found %s, expected %s", coll.ShardConfig.ShardCollection, nm)
		}
		if len(coll.ShardConfig.Key) == 0 {
			return fmt.Errorf("wrong value for 'shardConfig.key', can't be null and must be an object like {'_id': 'hashed'}, found: %v", coll.ShardConfig.Key)
		}
		// index to shard the collection
		index := config.Index{
			Name: "shardKey",
			Key:  coll.ShardConfig.Key,
		}
		err := c.Database.Run(bson.D{{Name: "createIndexes", Value: c.Name}, {Name: "indexes", Value: [1]config.Index{index}}}, &result)
		if err != nil {
			return fmt.Errorf("couldn't create shard key with index config %v\n\tcause: %s", index.Key, err.Error())
		}
		if !result.Ok {
			return fmt.Errorf("couldn't create shard key with index config %v\n\tcause: %s", index.Key, result.ErrMsg)
		}
		err = d.session.Run(coll.ShardConfig, &result)
		if err != nil {
			return fmt.Errorf("couldn't create sharded collection. Make sure that sharding is enabled,\n see https://docs.mongodb.com/manual/reference/command/enableSharding/#dbcmd.enableSharding for details\n\tcause: %s", err.Error())
		}
		if !result.Ok {
			return fmt.Errorf("couldn't create sharded collection \n\tcause: %s", result.ErrMsg)
		}
	}
	return nil
}

type rawChunk struct {
	documents  []bson.Raw
	nbToInsert int
}

func (d *datagen) fillCollection(coll *config.Collection) error {

	encoder := generators.NewEncoder(4)
	generator, err := generators.CreateGenerator(coll.Content, d.ShortName, coll.Count, d.version, encoder)
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

	task := make(chan *rawChunk, taskBufferSize)
	errs := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(nbInsertingGoRoutines)
	// start a new progressbar to display progress in terminal
	bar := pb.ProgressBarTemplate(`{{counters .}} {{ bar . "[" "=" ">" " " "]"}} {{percent . }}   {{speed . "%s doc/s" }}   {{rtime . "%s"}}          `).Start(int(coll.Count))
	bar.SetWriter(d.out)
	// start goroutines to bulk insert documents in MongoDB
	for i := 0; i < nbInsertingGoRoutines; i++ {
		go func() {
			defer wg.Done()
			// get a session with a distinct socket for each worker
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
					case errs <- fmt.Errorf("exception occurred during bulk insert:\n\tcause: %v", err):
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
	// counter for already generated documents
	count := int32(0)
	// start bson.Raw generation to feed the task channel
	for count < coll.Count {
		select {
		case <-ctx.Done(): // if an error occurred in one of the 'inserting' goroutines, close the channel
			close(task)
			bar.Finish()
		default:
		}
		rc := pool.Get().(*rawChunk)
		rc.nbToInsert = 1000
		if coll.Count-count < 1000 {
			rc.nbToInsert = int(coll.Count - count)
		}
		for i := 0; i < rc.nbToInsert; i++ {
			generator.Value()
			if len(rc.documents[i].Data) < len(encoder.Data) {
				for j := len(rc.documents[i].Data); j < len(encoder.Data); j++ {
					rc.documents[i].Data = append(rc.documents[i].Data, byte(0))
				}
			} else {
				rc.documents[i].Data = rc.documents[i].Data[0:len(encoder.Data)]
			}
			copy(rc.documents[i].Data, encoder.Data)
		}
		count += int32(rc.nbToInsert)
		bar.Add(rc.nbToInsert)
		task <- rc
	}
	close(task)

	wg.Wait()
	bar.Finish()
	// if an error occurs in one of the goroutines, return this error,
	// otherwise return nil
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
func (d *datagen) updateWithAggregators(coll *config.Collection) error {
	aggArr, err := generators.NewAggregatorFromMap(coll.Content, d.ShortName)
	if err != nil {
		return err
	}
	if len(aggArr) == 0 {
		return nil
	}
	fmt.Fprintf(d.out, "Generating aggregated data for collection %v\n", coll.Name)
	bar := pb.ProgressBarTemplate(`{{counters .}} {{ bar . "[" "=" ">" " " "]"}} {{percent . }}                          `).Start(int(coll.Count))
	bar.SetWriter(d.out)
	// aggregation might be very long, so make sure the connection won't timeout
	d.session.SetSocketTimeout(time.Duration(30) * time.Minute)
	c := d.session.DB(coll.DB).C(coll.Name)

	for _, agg := range aggArr {
		bulk := c.Bulk()
		bulk.Unordered()
		localVar := "_id"
		localKey := "_id"
		for k, v := range agg.Query {
			vStr := fmt.Sprintf("%v", v)
			if len(vStr) >= 2 && vStr[:2] == "$$" {
				localVar = vStr[2:]
				localKey = k
			}
		}

		var result struct {
			Values []interface{} `bson:"values"`
		}
		err := c.Database.Run(bson.D{{Name: "distinct", Value: c.Name}, {Name: "key", Value: localVar}}, &result)
		if err != nil {
			return fmt.Errorf("fail to get distinct values for local field %v: %v", localVar, err)
		}

		switch agg.Mode {
		case generators.CountAggregator:
			var r struct {
				N int32 `bson:"n"`
			}
			for _, v := range result.Values {
				command := bson.D{{Name: "count", Value: agg.Collection}}
				if agg.Query != nil {
					agg.Query[localKey] = v
					command = append(command, bson.DocElem{Name: "query", Value: agg.Query})
				}

				err := c.Database.Session.DB(agg.Database).Run(command, &r)
				if err != nil {
					return fmt.Errorf("couldn't count documents: %v", err)
				}
				bulk.Update(bson.M{localVar: v}, bson.M{"$set": bson.M{agg.K: r.N}})
			}
		case generators.ValueAggregator:
			for _, v := range result.Values {
				agg.Query[localKey] = v

				err = c.Database.Session.DB(agg.Database).Run(bson.D{
					{Name: "distinct", Value: agg.Collection},
					{Name: "key", Value: agg.Field},
					{Name: "query", Value: agg.Query}}, &result)

				if err != nil {
					return fmt.Errorf("aggregation failed for distinct values: %v", err)
				}
				bulk.Update(bson.M{localVar: v}, bson.M{"$set": bson.M{agg.K: result.Values}})
			}
		case generators.BoundAggregator:
			res := bson.M{}
			for _, v := range result.Values {
				agg.Query[localKey] = v
				agg.Query[agg.Field] = bson.M{"$ne": nil}
				bound := bson.M{}

				pipeline := []bson.M{{"$match": agg.Query},
					{"$sort": bson.M{agg.Field: 1}},
					{"$limit": 1},
					{"$project": bson.M{"min": "$" + agg.Field}}}
				err = c.Database.C(agg.Collection).Pipe(pipeline).One(&res)
				if err != nil {
					return fmt.Errorf("aggregation failed for lower bound: %v", err)
				}
				bound["m"] = res["min"]
				pipeline = []bson.M{{"$match": agg.Query},
					{"$sort": bson.M{agg.Field: -1}},
					{"$limit": 1},
					{"$project": bson.M{"max": "$" + agg.Field}}}
				err = c.Database.C(agg.Collection).Pipe(pipeline).One(&res)
				if err != nil {
					return fmt.Errorf("aggregation failed for higher bound: %v", err)
				}
				bound["M"] = res["max"]
				bulk.Update(bson.M{localVar: v}, bson.M{"$set": bson.M{agg.K: bound}})
			}
		}
		bar.Add(int(coll.Count) / len(aggArr))
		_, err = bulk.Run()
		if err != nil {
			return fmt.Errorf("bulk update failed for aggregator %v : %v", agg.K, err)
		}
	}
	bar.Finish()
	return nil
}

// create index on generated collections
func (d *datagen) ensureIndex(coll *config.Collection) error {
	if len(coll.Indexes) == 0 {
		fmt.Printf("No index to build for collection %s\n\n", coll.Name)
		return nil
	}
	fmt.Fprintf(d.out, "Building indexes for collection %s...\n", coll.Name)

	c := d.session.DB(coll.DB).C(coll.Name)
	err := c.DropAllIndexes()
	if err != nil {
		return fmt.Errorf("error while dropping index for collection %s:\n\tcause: %s", coll.Name, err.Error())
	}
	// avoid timeout when building indexes
	d.session.SetSocketTimeout(time.Duration(30) * time.Minute)
	// create the new indexes
	result := struct {
		ErrMsg string
		Ok     bool
	}{}
	err = c.Database.Run(bson.D{{Name: "createIndexes", Value: c.Name}, {Name: "indexes", Value: coll.Indexes}}, &result)
	if err != nil {
		return fmt.Errorf("error while building indexes for collection %s:\n\tcause: %s", coll.Name, err.Error())
	}
	if !result.Ok {
		return fmt.Errorf("error while building indexes for collection %s:\n\tcause: %s", coll.Name, result.ErrMsg)
	}
	color.New(color.FgGreen).Fprintf(d.out, "Building indexes for collection %s done\n\n", coll.Name)
	return nil
}

func (d *datagen) printCollStats(coll *config.Collection) error {
	c := d.session.DB(coll.DB).C(coll.Name)
	stats := struct {
		Count      int64  `bson:"count"`
		AvgObjSize int64  `bson:"avgObjSize"`
		IndexSizes bson.M `bson:"indexSizes"`
	}{}
	err := c.Database.Run(bson.D{{Name: "collStats", Value: c.Name}, {Name: "scale", Value: 1024}}, &stats)
	if err != nil {
		return fmt.Errorf("couldn't get stats for collection %s \n\tcause: %s ", c.Name, err.Error())
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

// print the error in red and exit
func printErrorAndExit(err error) {
	color.Red("ERROR: %v", err)
	os.Exit(1)
}

// General struct that stores global options from command line args
type General struct {
	Help    bool `long:"help" description:"show this help message"`
	Version bool `short:"v" long:"version" description:"print the tool version and exit"`
}

// Connection struct that stores info on connection from command line args
type Connection struct {
	Host     string `short:"h" long:"host" value-name:"<hostname>" description:"mongodb host to connect to" default:"127.0.0.1"`
	Port     string `long:"port" value-name:"<port>" description:"server port" default:"27017"`
	UserName string `short:"u" long:"username" value-name:"<username>" description:"username for authentification"`
	Password string `short:"p" long:"password" value-name:"<password>" description:"password for authentification"`
}

// Config struct that stores info on config file from command line args
type Config struct {
	ConfigFile      string `short:"f" long:"file" value-name:"<configfile>" description:"JSON config file. This field is required"`
	IndexOnly       bool   `short:"i" long:"indexonly" description:"if present, mgodatagen will just try to rebuild index"`
	ShortName       bool   `short:"s" long:"shortname" description:"if present, JSON keys in the documents will be reduced\n to the first two letters only ('name' => 'na')"`
	Append          bool   `short:"a" long:"append" description:"if present, append documents to the collection without\n removing older documents or deleting the collection"`
	NumInsertWorker int    `short:"n" long:"numWorker" value-name:"<nb>" description:"number of concurrent workers inserting documents\n in database. Default is number of CPU+1"`
}

// Template struct that stores info on config file to generate
type Template struct {
	New string `long:"new" value-name:"<filename>" description:"create an empty configuration file"`
}

// Options struct to store flags from CLI
type Options struct {
	Template   `group:"template"`
	Config     `group:"configuration"`
	Connection `group:"connection infos"`
	General    `group:"general"`
}

type datagen struct {
	out     io.Writer
	session *mgo.Session
	version []int
	Options
}

func (d *datagen) generate(v *config.Collection) error {
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

func main() {
	var options Options
	p := flags.NewParser(&options, flags.Default&^flags.HelpFlag)
	_, err := p.Parse()
	if err != nil {
		fmt.Println("try mgodatagen --help for more informations")
		os.Exit(1)
	}
	if options.Help {
		fmt.Printf("mgodatagen version %s\n\n", version)
		p.WriteHelp(os.Stdout)
		os.Exit(0)
	}
	if options.Version {
		fmt.Printf("mgodatagen version %s\n", version)
		os.Exit(0)
	}
	if options.New != "" {
		err = createEmptyCfgFile(options.New)
		if err != nil {
			printErrorAndExit(fmt.Errorf("could not create an empty configuration file: %v", err))
		}
		os.Exit(0)
	}
	if options.ConfigFile == "" {
		printErrorAndExit(fmt.Errorf("No configuration file provided, try mgodatagen --help for more informations "))
	}

	fmt.Println("Parsing configuration file...")
	collectionList, err := config.CollectionList(options.ConfigFile)
	if err != nil {
		printErrorAndExit(err)
	}
	session, version, err := connectToDB(&options.Connection)
	if err != nil {
		printErrorAndExit(err)
	}
	defer session.Close()

	datagen := &datagen{
		out:     os.Stderr,
		session: session,
		Options: options,
		version: version,
	}

	for _, v := range collectionList {
		err = datagen.generate(&v)
		if err != nil {
			printErrorAndExit(err)
		}
	}
	color.Green("Done")
}
