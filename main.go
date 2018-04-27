package main

import (
	"os"

	"github.com/fatih/color"
	"github.com/jessevdk/go-flags"

	"github.com/feliixx/mgodatagen/datagen"
)

func main() {
	var options datagen.Options
	p := flags.NewParser(&options, flags.Default&^flags.HelpFlag)
	_, err := p.Parse()
	if err != nil {
		color.Red("invalid flags, try mgodatagen --help for more informations: %v", err)
		os.Exit(1)
	}
	if options.Help {
		p.WriteHelp(os.Stdout)
		os.Exit(0)
	}
	err = datagen.Generate(&options, os.Stdout)
	if err != nil {
		color.Red("%v", err)
		os.Exit(1)
	}
}
