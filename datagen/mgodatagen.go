// Package datagen used to generate millions of pseudo-random BSON documents
// and insert them into a Mongodb instance.
package datagen

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

const defaultTimeout = 10 * time.Second

// Generate creates a database according to specified options. Progress informations
// are send to out
func Generate(options *Options, out io.Writer) error {
	return run(options, out)
}

func run(options *Options, out io.Writer) error {
	if options.Quiet {
		out = ioutil.Discard
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
	content, err := ioutil.ReadFile(options.ConfigFile)
	if err != nil {
		return fmt.Errorf("File error: %v", err)
	}
	collections, err := ParseConfig(content, false)
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
		out:        out,
		session:    session,
		version:    version,
		mapRef:     make(map[int][][]byte),
		mapRefType: make(map[int]bsontype.Type),
		Options:    *options,
	}

	start := time.Now()
	defer printElapsedTime(out, start)

	for _, collection := range collections {
		err = dtg.generate(&collection)
		if err != nil {
			return err
		}
	}
	dtg.printStats(collections)

	return nil
}

type result struct {
	Ok     bool
	ErrMsg string
	Shards []bson.M
}

func handleCommandError(msg string, err error, r *result) error {
	m := err.Error()
	if !r.Ok {
		m = r.ErrMsg
	}
	return fmt.Errorf("%s\n  cause: %s", msg, m)
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
	fmt.Fprintf(out, "MongoDB server version %s\n\n", infos.Version)

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

func createEmptyCfgFile(filename string) error {
	_, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		fmt.Printf("file %s already exists, overwrite it ?  [y/n]: ", filename)
		response := make([]byte, 2)
		_, err := os.Stdin.Read(response)
		if err != nil {
			return fmt.Errorf("couldn't read from user, aborting %v", err)
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

	templateByte := []byte(`
[
    {
        "database": "dbName",
        "collection": "collName",
        "count": 1000,
        "content": {

        }
    }
]		
`)
	_, err = f.Write(templateByte[1:])
	return err
}

func printElapsedTime(out io.Writer, start time.Time) {
	elapsed := time.Since(start).Round(10 * time.Millisecond)
	fmt.Fprintf(out, "\nrun finished in %s\n", elapsed.String())
}
