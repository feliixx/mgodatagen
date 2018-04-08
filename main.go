// A small CLI tool to quickly generate millions of pseudo-random BSON documents
// and insert them into a Mongodb instance.
package main

import (
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/jessevdk/go-flags"
)

func main() {
	var options Options
	p := flags.NewParser(&options, flags.Default&^flags.HelpFlag)
	_, err := p.Parse()
	if err != nil {
		color.Red("invalid flags, try mgodatagen --help for more informations: %v", err)
		os.Exit(1)
	}
	if options.Help {
		fmt.Fprintf(os.Stdout, "mgodatagen version %s\n\n", version)
		p.WriteHelp(os.Stdout)
		os.Exit(0)
	}
	err = Mgodatagen(&options)
	if err != nil {
		color.Red("%v", err)
		os.Exit(1)
	}
}
