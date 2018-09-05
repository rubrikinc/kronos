package main

import (
	"context"
	"flag"
	"os"

	"github.com/scaledata/kronos/cli"
	"github.com/scaledata/kronos/kronosutil/log"
)

func main() {
	if len(os.Args) == 1 {
		os.Args = append(os.Args, "help")
	}

	flag.Parse()

	cli.RootCmd.SetArgs(flag.Args())
	if err := cli.RootCmd.Execute(); err != nil {
		log.Fatal(context.Background(), err)
	}
}
