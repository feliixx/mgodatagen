// Package datagen used to generate millions of pseudo-random BSON documents
// and insert them into a Mongodb instance.
package datagen

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"
)

// Generate creates documents from options. Logs and progress are send
// to logger
func Generate(options *Options, logger io.Writer) error {
	return run(options, logger)
}

func run(options *Options, logger io.Writer) error {

	if options.Quiet {
		logger = ioutil.Discard
	}
	if options.New != "" {
		return createEmptyCfgFile(options.New)
	}
	if options.ConfigFile == "" {
		return fmt.Errorf("no configuration file provided, try mgodatagen --help for more informations ")
	}
	if options.BatchSize > 1000 || options.BatchSize <= 0 {
		return fmt.Errorf("invalid value for -b | --batchsize: %v. BatchSize has to be between 1 and 1000", options.BatchSize)
	}
	if options.IndexOnly && options.IndexFirst {
		return errors.New("-i | --indexonly and -x | --indexfirst can't be present at the same time. Try to remove the -x | --indexfirst flag")
	}

	if options.IndexFirst {
		fmt.Fprint(logger, `WARNING: when -x | --indexfirst flag is set, all write errors are ignored.
Actual collection count may not match the 'count' specified in config file
`)
	}

	if options.Output == "" {
		options.Output = mongodbOutput
	}
	// if docs are written to stdout, do not pollute the output with logs
	if options.Output == stdoutOutput {
		logger = io.Discard
	}

	content, err := ioutil.ReadFile(options.ConfigFile)
	if err != nil {
		return fmt.Errorf("fail to read file %s\n  cause: %v", options.ConfigFile, err)
	}
	collections, err := ParseConfig(content, false)
	if err != nil {
		return err
	}

	writer, err := newWriter(options, logger)
	if err != nil {
		return err
	}

	start := time.Now()
	seed := options.Seed
	if seed == 0 {
		seed = uint64(start.Unix())
	}

	fmt.Fprintf(logger, "Using seed: %d\n\n", seed)
	err = writer.write(collections, seed)
	if err != nil {
		return err
	}

	printElapsedTime(logger, start)
	return nil
}

func printElapsedTime(out io.Writer, start time.Time) {
	elapsed := time.Since(start).Round(10 * time.Millisecond)
	fmt.Fprintf(out, "\nrun finished in %s\n", elapsed.String())
}

func createEmptyCfgFile(filename string) error {

	f, err := tryToCreateFile(filename)
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

func tryToCreateFile(filename string) (*os.File, error) {
	_, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		fmt.Printf("file %s already exists, overwrite it ?  [y/n]: ", filename)
		response := make([]byte, 2)
		_, err := os.Stdin.Read(response)
		if err != nil {
			return nil, fmt.Errorf("couldn't read from user, aborting: %v", err)
		}
		if string(response[0]) != "y" {
			return nil, errors.New("aborting")
		}
	}
	f, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("could not create file: %v", err)
	}
	return f, nil
}
