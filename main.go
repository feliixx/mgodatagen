package main

import (
	"fmt"
	"os"

	"github.com/feliixx/mgodatagen/datagen"

	"github.com/jessevdk/go-flags"
)

// Version of mgodatagen. Should be linked via ld_flags when compiling for binary release
//
// Use this to set version to last known tag:
//
//  go build -ldflags "-X main.Version=$(git describe --tags $(git rev-list --tags --max-count=1))"
var Version string = "v0.11.0"

func main() {
	var options datagen.Options
	p := flags.NewParser(&options, flags.Default&^flags.HelpFlag)
	p.Usage = "-f config_file.json"
	_, err := p.Parse()
	if err != nil {
		fmt.Println("try mgodatagen --help for more informations")
		os.Exit(1)
	}
	if options.Help {
		p.WriteHelp(os.Stdout)
		os.Exit(0)
	}

	if options.Version {
		fmt.Printf("mgodatagen %s\n", Version)
		os.Exit(0)
	}

	err = datagen.Generate(&options, os.Stdout)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
