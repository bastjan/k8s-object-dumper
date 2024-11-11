package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"rsc.io/getopt"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/bastjan/k8s-object-dumper/internal/pkg/discovery"
	"github.com/bastjan/k8s-object-dumper/internal/pkg/dumper"
)

func main() {
	var dir string
	var batchSize int64
	flag.StringVar(&dir, "dir", "", "Directory to dump objects into")
	flag.Int64Var(&batchSize, "batch-size", 500, "Batch size for listing objects")
	getopt.Alias("d", "dir")

	getopt.Parse()

	df := dumper.DumpToWriter(os.Stdout)
	if dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "failed to create directory %s: %v\n", dir, err)
			os.Exit(1)
		}
		d, err := dumper.NewDirDumper(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create directory dumper: %v\n", err)
			os.Exit(1)
		}
		defer d.Close()
		df = d.Dump
	}

	conf, err := ctrl.GetConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get Kubernetes config: %v", err)
	}

	if err := discovery.DiscoverObjects(context.Background(), conf, df, discovery.DiscoveryOptions{
		BatchSize: batchSize,
		LogWriter: os.Stderr,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "failed to dump some or all objects: %+v\n", err)
		os.Exit(1)
	}
}
