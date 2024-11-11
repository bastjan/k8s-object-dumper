package discovery

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"

	"go.uber.org/multierr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

type DiscoveryOptions struct {
	BatchSize int64
	LogWriter io.Writer
}

// GetBatchSize returns the set batch size for listing objects or the default.
func (opts DiscoveryOptions) GetBatchSize() int64 {
	if opts.BatchSize == 0 {
		return 500
	}
	return opts.BatchSize
}

// GetLogWriter returns the set batch size for listing objects or io.Discard as default.
func (opts DiscoveryOptions) GetLogWriter() io.Writer {
	if opts.LogWriter == nil {
		return io.Discard
	}
	return opts.LogWriter
}

// DiscoverObjects discovers all objects in the cluster and calls the provided callback for each list of objects.
// The callback can be called multiple times with the same
func DiscoverObjects(ctx context.Context, conf *rest.Config, cb func(*unstructured.UnstructuredList) error, opts DiscoveryOptions) error {
	batchSize := opts.GetBatchSize()
	logWriter := opts.GetLogWriter()

	dc, err := discovery.NewDiscoveryClientForConfig(conf)
	if err != nil {
		return fmt.Errorf("failed to create discovery client: %w", err)
	}
	dynClient, err := dynamic.NewForConfig(conf)
	if err != nil {
		return fmt.Errorf("failed to create dynamic client: %w", err)
	}

	sprl, err := dc.ServerPreferredResources()
	if err != nil {
		return fmt.Errorf("failed to get server preferred resources: %w", err)
	}

	fmt.Fprintln(logWriter, "Discovered resources:")
	for _, re := range sprl {
		fmt.Fprintln(logWriter, re.GroupVersion)
		for _, r := range re.APIResources {
			fmt.Fprintln(logWriter, "  ", r.Kind)
		}
	}

	var errors []error
	for _, re := range sprl {
		for _, r := range re.APIResources {
			res := groupVersionFromString(re.GroupVersion).WithResource(r.Name)
			if !slices.Contains(r.Verbs, "list") {
				fmt.Fprintf(logWriter, "skipping %s: no list verb\n", res)
				continue
			}

			continueKey := ""
			for {
				l, err := dynClient.Resource(res).List(ctx, metav1.ListOptions{
					Limit:    batchSize,
					Continue: continueKey,
				})
				if err != nil {
					errors = append(errors, fmt.Errorf("failed to list %s: %w", res, err))
					break
				}
				if err := cb(l); err != nil {
					errors = append(errors, fmt.Errorf("failed to dump %s: %w", res, err))
				}
				if l.GetContinue() == "" {
					break
				}
				continueKey = l.GetContinue()
			}
		}
	}

	return multierr.Combine(errors...)
}

func groupVersionFromString(s string) schema.GroupVersion {
	parts := strings.Split(s, "/")
	if len(parts) == 1 {
		return schema.GroupVersion{Version: parts[0]}
	}
	return schema.GroupVersion{Group: parts[0], Version: parts[1]}
}
