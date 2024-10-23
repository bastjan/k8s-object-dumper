package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	ctrl "sigs.k8s.io/controller-runtime"
)

func main() {
	conf := ctrl.GetConfigOrDie()
	dc := discovery.NewDiscoveryClientForConfigOrDie(conf)
	dynClient := dynamic.NewForConfigOrDie(conf)

	res, err := dc.ServerPreferredResources()
	if err != nil {
		panic(fmt.Errorf("failed to get server preferred resources: %w", err))
	}

	fmt.Fprintln(os.Stderr, "Discovered resources:")
	for _, re := range res {
		fmt.Fprintln(os.Stderr, re.GroupVersion)
		for _, r := range re.APIResources {
			fmt.Fprintln(os.Stderr, "  ", r.Kind)
		}
	}

	for _, re := range res {
		for _, r := range re.APIResources {
			res := groupVersionFromString(re.GroupVersion).WithResource(r.Name)
			if !slices.Contains(r.Verbs, "list") {
				fmt.Fprintf(os.Stderr, "skipping %s: no list verb\n", res)
				continue
			}
			l, err := dynClient.Resource(res).List(context.TODO(), metav1.ListOptions{})
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to list %s: %v\n", res, err)
				continue
			}
			if err := json.NewEncoder(os.Stdout).Encode(l); err != nil {
				fmt.Fprintf(os.Stderr, "failed to encode %s: %v\n", res, err)
			}
		}
	}
}

func groupVersionFromString(s string) schema.GroupVersion {
	parts := strings.Split(s, "/")
	if len(parts) == 1 {
		return schema.GroupVersion{Version: parts[0]}
	}
	return schema.GroupVersion{Group: parts[0], Version: parts[1]}
}
