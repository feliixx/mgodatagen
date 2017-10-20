// A small CLI tool to quickly generate millions of pseudo-random BSON documents
// and insert them into a Mongodb instance.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/jessevdk/go-flags"
	"gopkg.in/cheggaaa/pb.v2"

	cf "github.com/feliixx/mgodatagen/config"
	rg "github.com/feliixx/mgodatagen/generatorsBSON"
)

const (
	version     = "0.3" // current version of mgodatagen
	maxBSONSize = 15 * 1000 * 1000
)

// dResult stores the result of a `distinct` command
type dResult struct {
	Values []interface{} `bson:"values"`
	Ok     bool          `bson:"ok"`
}

// get a connection from Connection args
func connectToDB(conn *Connection) (*mgo.Session, error) {
	fmt.Printf("Connecting to mongodb://%s:%s\n\n", conn.Host, conn.Port)
	url := "mongodb://"
	if conn.UserName != "" && conn.Password != "" {
		url += conn.UserName + ":" + conn.Password + "@"
	}
	session, err := mgo.Dial(url + conn.Host + ":" + conn.Port)
	if err != nil {
		return nil, fmt.Errorf("connection failed:\n\tcause: %s", err.Error())
	}
	infos, err := session.BuildInfo()
	if err != nil {
		return nil, fmt.Errorf("couldn't get mongodb version:\n\tcause: %s", err.Error())
	}
	fmt.Printf("mongodb server version %s\ngit version %s\nOpenSSL version %s\n\n", infos.Version, infos.GitVersion, infos.OpenSSLVersion)
	result := struct {
		Ok     bool
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
	return session, nil
}

// create a collection with specific options
func createCollection(coll *cf.Collection, session *mgo.Session, indexOnly bool, appendToColl bool) (*mgo.Collection, error) {
	c := session.DB(coll.DB).C(coll.Name)
	// if indexOnly or append mode, just return the collection as it already exists
	if indexOnly || appendToColl {
		return c, nil
	}
	// drop the collection before inserting new document. Ignore the error
	// if the collection does not exists
	c.DropCollection()
	fmt.Printf("Creating collection %s...\n", coll.Name)
	// if a compression level is specified, explicitly create the collection with the selected
	// compression level
	if coll.CompressionLevel != "" {
		err := c.Create(&mgo.CollectionInfo{StorageEngine: bson.M{"wiredTiger": bson.M{"configString": "block_compressor=" + coll.CompressionLevel}}})
		if err != nil {
			return nil, fmt.Errorf("coulnd't create collection with compression level %s:\n\tcause: %s", coll.CompressionLevel, err.Error())
		}
	}
	// if the collection has to be sharded
	if coll.ShardConfig.ShardCollection != "" {
		result := struct {
			ErrMsg string
			Ok     bool
		}{}
		// check that the config is correct
		nm := c.Database.Name + "." + c.Name
		if coll.ShardConfig.ShardCollection != nm {
			return nil, fmt.Errorf("wrong value for 'shardConfig.shardCollection', should be <database>.<collection>: found %s, expected %s", coll.ShardConfig.ShardCollection, nm)
		}
		if len(coll.ShardConfig.Key) == 0 {
			return nil, fmt.Errorf("wrong value for 'shardConfig.key', can't be null and must be an object like {'_id': 'hashed'}, found: %v", coll.ShardConfig.Key)
		}
		// index to shard the collection
		index := cf.Index{Name: "shardKey", Key: coll.ShardConfig.Key}
		err := c.Database.Run(bson.D{{Name: "createIndexes", Value: c.Name}, {Name: "indexes", Value: [1]cf.Index{index}}}, &result)
		if err != nil {
			return nil, fmt.Errorf("couldn't create shard key with index config %v\n\tcause: %s", index.Key, err.Error())
		}
		if !result.Ok {
			return nil, fmt.Errorf("couldn't create shard key with index config %v\n\tcause: %s", index.Key, result.ErrMsg)
		}
		err = session.Run(coll.ShardConfig, &result)
		if err != nil {
			return nil, fmt.Errorf("couldn't create sharded collection. Make sure that sharding is enabled,\n see https://docs.mongodb.com/manual/reference/command/enableSharding/#dbcmd.enableSharding for details\n\tcause: %s", err.Error())
		}
		if !result.Ok {
			return nil, fmt.Errorf("couldn't create sharded collection \n\tcause: %s", result.ErrMsg)
		}
	}
	return c, nil
}

type bufferedBulkInserter struct {
	Bulk     *mgo.Bulk
	DocCount int
}

// insert documents in DB, and then close the session
func insertInDB(coll *cf.Collection, c *mgo.Collection, shortNames bool, numInsertWorker int) error {
	// create a generator
	rndSrc := rand.NewSource(time.Now().UnixNano())

	encoder := &rg.Encoder{Data: make([]byte, 4), DocCount: int32(0), R: rand.New(rndSrc), Src: rndSrc}
	generator, err := rg.CreateGenerator(coll.Content, shortNames, coll.Count, encoder)
	if err != nil {
		return err
	}
	// Create a rand.Rand object to generate our random values
	//source := rg.NewRandSource()

	// number of routines inserting documents simultaneously in database
	nbInsertingGoRoutines := runtime.NumCPU() + 1
	if numInsertWorker > 0 {
		nbInsertingGoRoutines = numInsertWorker
	}
	// size of the buffered channel for docs to insert
	docBufferSize := 100
	// for really small insert, use only one goroutine and reduce the buffered channel size
	if coll.Count < 3000 {
		nbInsertingGoRoutines = 1
		docBufferSize = 1
	}
	// To make insertion faster, buffer the generated documents
	// and push them to a channel.
	record := make(chan bson.Raw, docBufferSize)
	// A channel to get error from goroutines
	errs := make(chan error, 1)
	// use context to handle errors in goroutines. If an error occurs in a goroutine,
	// all goroutines should terminate and the buffered channel should be closed.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// create a waitGroup to make sure all the goroutines
	// have ended before returning
	var wg sync.WaitGroup
	wg.Add(nbInsertingGoRoutines)
	// start a new progressbar to display progress in terminal
	bar := pb.ProgressBarTemplate(`{{counters .}} {{ bar . "[" "=" ">" " " "]"}} {{percent . }}   {{speed . "%s doc/s" }}   {{rtime . "%s"}}          `).Start(int(coll.Count))
	// start goroutines to bulk insert documents in MongoDB
	for i := 0; i < nbInsertingGoRoutines; i++ {
		go func() {
			defer wg.Done()
			// get a session for each worker
			s := c.Database.Session.Copy()
			defer s.Close()
			coll := c.With(s)

			b := bufferedBulkInserter{
				Bulk:     coll.Bulk(),
				DocCount: 0,
			}
			// do not check error for each insert, but only one
			// all documents in the Bulk have been inserted
			b.Bulk.Unordered()

			for r := range record {
				// if an error occurs in one of the goroutine, 'return' is called which trigger
				// wg.Done() ==> the goroutine stops
				select {
				case <-ctx.Done():
					return
				default:
				}
				if b.DocCount >= 1000 {
					_, err := b.Bulk.Run()
					if err != nil {
						// if the bulk insert fails, push the error to the error channel
						// so that we can use it from the main thread
						select {
						case errs <- fmt.Errorf("exception occurred during bulk insert:\n\tcause: %s", err.Error()):
						default:
						}
						// cancel the context to terminate goroutine and stop the feeding of the
						// buffered channel
						cancel()
						return
					}
					b.Bulk = coll.Bulk()
					b.Bulk.Unordered()
					b.DocCount = int(0)
				}
				b.Bulk.Insert(r)
				b.DocCount++
			}
			// if there is some documents remaining in the bulk
			if b.DocCount > 0 {
				_, err := b.Bulk.Run()
				if err != nil {
					// if the bulk insert fails, push the error to the error channel
					// so that we can use it from the main thread
					select {
					case errs <- fmt.Errorf("exception occurred during bulk insert:\n\tcause: %s", err.Error()):
					default:
					}
					// cancel the context to terminate goroutine and stop the feeding of the
					// buffered channel
					cancel()
					return
				}
			}
		}()

		// sleep to prevent all threads from inserting at the same time at start
		time.Sleep(time.Duration(i) * 10 * time.Millisecond)
	}
	// counter for already generated documents
	count := int32(0)
	// start bson.Raw generation to feed the buffered channel
	for count < coll.Count {
		select {
		case <-ctx.Done(): // if an error occurred in one of the 'inserting' goroutines, close the channel
			close(record)
			bar.Finish()
			// return <-errs
		default:
		}
		// push generated bson.Raw to the buffered channel
		for encoder.DocCount < 1000 && count+encoder.DocCount < coll.Count {
			generator.Value()
			data := make([]byte, len(encoder.Data))
			copy(data, encoder.Data)
			record <- bson.Raw{Data: data, Kind: bson.ElementDocument}
		}
		count += encoder.DocCount
		//count += encoder.DocCount
		bar.Add(int(encoder.DocCount))
		encoder.DocCount = int32(0)
	}
	close(record)
	// wait for goroutines to end
	wg.Wait()
	bar.Finish()
	// if an error occurs in one of the goroutines, return this error,
	// otherwise return nil
	if ctx.Err() != nil {
		return <-errs
	}
	color.Green("Generating collection %s done\n", coll.Name)
	return nil
}

//Update documents with pre-computed aggregations
func updateWithAggregators(coll *cf.Collection, c *mgo.Collection, shortNames bool) error {
	aggArr, err := rg.NewAggregatorFromMap(coll.Content, shortNames)
	if err != nil {
		return err
	}
	if len(aggArr) == 0 {
		return nil
	}
	fmt.Printf("Generating aggregated data for collection %v\n", c.Name)
	bar := pb.ProgressBarTemplate(`{{counters .}} {{ bar . "[" "=" ">" " " "]"}} {{percent . }}          `).Start(int(coll.Count) * len(aggArr))
	// aggregation might be very long, so make sure the connection won't timeout
	c.Database.Session.SetSocketTimeout(time.Duration(30) * time.Minute)
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
		var result dResult
		err = c.Database.Run(bson.D{{Name: "distinct", Value: c.Name}, {Name: "key", Value: localVar}}, &result)
		if err != nil {
			return err
		}
		switch agg.Mode {
		case rg.CountAggregator:
			var r struct {
				N  int32 `bson:"n"`
				Ok bool  `bson:"ok"`
			}
			for _, v := range result.Values {
				command := bson.D{{Name: "count", Value: agg.Collection}}
				if agg.Query != nil {
					agg.Query[localKey] = v
					command = append(command, bson.DocElem{Name: "query", Value: agg.Query})
				}

				err := c.Database.Session.DB(agg.Database).Run(command, &r)
				if err != nil {
					return err
				}
				bulk.Update(bson.M{localVar: v}, bson.M{"$set": bson.M{agg.K: r.N}})
			}
		case rg.ValueAggregator:
			var r dResult
			for _, v := range result.Values {
				agg.Query[localKey] = v

				err = c.Database.Session.DB(agg.Database).Run(bson.D{
					{Name: "distinct", Value: agg.Collection},
					{Name: "key", Value: agg.Field},
					{Name: "query", Value: agg.Query}}, &r)

				if err != nil {
					return err
				}

				bulk.Update(bson.M{localVar: v}, bson.M{"$set": bson.M{agg.K: r.Values}})
			}
		case rg.BoundAggregator:
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
					return err
				}
				bound["m"] = res["min"]
				pipeline = []bson.M{{"$match": agg.Query},
					{"$sort": bson.M{agg.Field: -1}},
					{"$limit": 1},
					{"$project": bson.M{"max": "$" + agg.Field}}}
				err = c.Database.C(agg.Collection).Pipe(pipeline).One(&res)
				if err != nil {
					return err
				}
				bound["M"] = res["max"]
				bulk.Update(bson.M{localVar: v}, bson.M{"$set": bson.M{agg.K: bound}})
			}
		}
		bar.Add(int(coll.Count))
		_, err = bulk.Run()
		if err != nil && err.Error() == "not found" {
			return err
		}
	}
	bar.Finish()
	return nil
}

// create index on generated collections. Use run command as there is no wrapper
// like dropIndexes() in current mgo driver.
func ensureIndex(coll *cf.Collection, c *mgo.Collection) error {
	if len(coll.Indexes) == 0 {
		fmt.Printf("No index to build for collection %s\n\n", coll.Name)
		return nil
	}
	fmt.Printf("Building indexes for collection %s...\n", coll.Name)

	err := c.DropAllIndexes()
	if err != nil {
		return fmt.Errorf("error while dropping index for collection %s:\n\tcause: %s", coll.Name, err.Error())
	}
	// avoid timeout when building indexes
	c.Database.Session.SetSocketTimeout(time.Duration(30) * time.Minute)
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
	color.Green("Building indexes for collection %s done\n\n", coll.Name)
	return nil
}

func printCollStats(c *mgo.Collection) error {
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
	fmt.Printf("Stats for collection %s:\n\t - doc count: %v\n\t - average object size: %v bytes\n\t - indexes: %s\n", c.Name, stats.Count, stats.AvgObjSize, indexString)
	return nil
}

// print the error in red and exit
func printErrorAndExit(err error) {
	color.Red("ERROR: %s", err.Error())
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

// Options struct to store flags from CLI
type Options struct {
	Config     `group:"configuration"`
	Connection `group:"connection infos"`
	General    `group:"general"`
}

func main() {

	// struct to store command line args
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

	// if -v|--version print version and exit
	if options.Version {
		fmt.Printf("mgodatagen version %s\n", version)
		os.Exit(0)
	}
	if options.ConfigFile == "" {
		printErrorAndExit(fmt.Errorf("No configuration file provided, try mgodatagen --help for more informations "))
	}
	fmt.Println("Parsing configuration file...")
	collectionList, err := cf.CollectionList(options.ConfigFile)
	if err != nil {
		printErrorAndExit(err)
	}
	session, err := connectToDB(&options.Connection)
	if err != nil {
		printErrorAndExit(err)
	}
	defer session.Close()
	// iterate over collection config
	for _, v := range collectionList {
		// create the collection
		c, err := createCollection(&v, session, options.IndexOnly, options.Append)
		if err != nil {
			printErrorAndExit(err)
		}
		// insert docs in database
		if !options.IndexOnly {
			err = insertInDB(&v, c, options.ShortName, options.NumInsertWorker)
			if err != nil {
				printErrorAndExit(err)
			}
		}
		// create indexes on the collection
		err = ensureIndex(&v, c)
		if err != nil {
			printErrorAndExit(err)
		}
		err = printCollStats(c)
		if err != nil {
			printErrorAndExit(err)
		}
	}
	color.Green("Done")
}
