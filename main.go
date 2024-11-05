package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"go.uber.org/multierr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"rsc.io/getopt"
	ctrl "sigs.k8s.io/controller-runtime"
)

type dumperFunc func(*unstructured.UnstructuredList) error

func main() {
	var dir string
	var batchSize int64
	flag.StringVar(&dir, "dir", "", "Directory to dump objects into")
	flag.Int64Var(&batchSize, "batch-size", 500, "Batch size for listing objects")
	getopt.Alias("d", "dir")

	getopt.Parse()

	df := dumpJSONToWriter(os.Stdout)
	if dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "failed to create directory %s: %v\n", dir, err)
			os.Exit(1)
		}
		d, err := newDirDumper(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create directory dumper: %v\n", err)
			os.Exit(1)
		}
		defer d.Close()
		df = d.Dump
	}

	dumpObjects(df, batchSize, os.Stderr)
}

func dumpObjects(dumper dumperFunc, batchSize int64, logWriter io.Writer) error {
	conf, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("failed to get Kubernetes config: %w", err)
	}
	dc, err := discovery.NewDiscoveryClientForConfig(conf)
	if err != nil {
		return fmt.Errorf("failed to create discovery client: %w", err)
	}
	dynClient, err := dynamic.NewForConfig(conf)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	res, err := dc.ServerPreferredResources()
	if err != nil {
		return fmt.Errorf("failed to get server preferred resources: %w", err)
	}

	fmt.Fprintln(logWriter, "Discovered resources:")
	for _, re := range res {
		fmt.Fprintln(logWriter, re.GroupVersion)
		for _, r := range re.APIResources {
			fmt.Fprintln(logWriter, "  ", r.Kind)
		}
	}

	for _, re := range res {
		for _, r := range re.APIResources {
			res := groupVersionFromString(re.GroupVersion).WithResource(r.Name)
			if !slices.Contains(r.Verbs, "list") {
				fmt.Fprintf(logWriter, "skipping %s: no list verb\n", res)
				continue
			}

			continueKey := ""
			for {
				l, err := dynClient.Resource(res).List(context.TODO(), metav1.ListOptions{
					Limit:    batchSize,
					Continue: continueKey,
				})
				if err != nil {
					fmt.Fprintf(logWriter, "failed to list %s: %v\n", res, err)
					continue
				}
				if err := dumper(l); err != nil {
					fmt.Fprintf(logWriter, "failed to dump %s: %v\n", res, err)
				}
				if l.GetContinue() == "" {
					break
				}
				continueKey = l.GetContinue()
			}
		}
	}

	return nil
}

func dumpJSONToWriter(w io.Writer) dumperFunc {
	return func(l *unstructured.UnstructuredList) error {
		return json.NewEncoder(w).Encode(l)
	}
}

// dirDumper writes objects to a directory.
// Must be initialized with newDirDumper.
// Must be closed after use.
type dirDumper struct {
	dir string

	openFiles map[string]*os.File
	sharedBuf *bytes.Buffer
}

// newDirDumper creates a new dirDumper that writes objects to the given directory.
// The directory will be created if it does not exist.
// If the directory cannot be created, an error is returned.
func newDirDumper(dir string) (*dirDumper, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %q: %w", dir, err)
	}
	return &dirDumper{
		dir:       dir,
		openFiles: make(map[string]*os.File),
		sharedBuf: new(bytes.Buffer),
	}, nil
}

// Close closes the dirDumper and all open files.
// The dirDumper cannot be used after it is closed.
func (d *dirDumper) Close() error {
	var errs []error
	for _, f := range d.openFiles {
		if err := f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return multierr.Combine(errs...)
}

// Dump writes the objects in the list to the directory.
// The objects are written to the directory in two ways:
// - All objects are written to a file named objects-<kind>.json
// - Objects with a namespace are written to a directory named split/<namespace> with two files:
//   - __all__.json contains all objects in the namespace
//   - <kind>.json contains all objects of the kind in the namespace
//
// If an object cannot be written, an error is returned.
// This method is not safe for concurrent use.
func (d *dirDumper) Dump(l *unstructured.UnstructuredList) error {
	buf := d.sharedBuf
	var errs []error
	for _, o := range l.Items {
		buf.Reset()
		if err := json.NewEncoder(buf).Encode(o); err != nil {
			errs = append(errs, fmt.Errorf("failed to encode object: %w", err))
			continue
		}
		p := buf.Bytes()

		if err := d.writeToFile(fmt.Sprintf("%s/objects-%s.json", d.dir, o.GetKind()), p); err != nil {
			errs = append(errs, err)
		}

		if o.GetNamespace() == "" {
			continue
		}

		if err := d.writeToFile(fmt.Sprintf("%s/split/%s/__all__.json", d.dir, o.GetNamespace()), p); err != nil {
			errs = append(errs, err)
		}
		if err := d.writeToFile(fmt.Sprintf("%s/split/%s/%s.json", d.dir, o.GetNamespace(), o.GetKind()), p); err != nil {
			errs = append(errs, err)
		}
	}
	return multierr.Combine(errs...)
}

func (d *dirDumper) writeToFile(path string, b []byte) error {
	f, err := d.file(path)
	if err != nil {
		return fmt.Errorf("failed to open file for copying: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		return fmt.Errorf("failed to copy to file: %w", err)
	}
	return nil
}

func (d *dirDumper) file(path string) (*os.File, error) {
	f, ok := d.openFiles[path]
	if ok {
		return f, nil
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %q: %w", dir, err)
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create file %q: %w", path, err)
	}
	d.openFiles[path] = f
	return f, nil
}

func groupVersionFromString(s string) schema.GroupVersion {
	parts := strings.Split(s, "/")
	if len(parts) == 1 {
		return schema.GroupVersion{Version: parts[0]}
	}
	return schema.GroupVersion{Group: parts[0], Version: parts[1]}
}
