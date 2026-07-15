package main

import (
	"fmt"
	"os"

	"github.com/llm-d/llm-d-async/pkg/server"
	// Register the built-in request body-transform plugins.
	_ "github.com/llm-d/llm-d-async/pkg/asyncworker/transform/gcsmultipart"
	"github.com/spf13/pflag"
	ctrl "sigs.k8s.io/controller-runtime"
)

func main() {
	opts := server.NewOptions()
	opts.AddFlags(pflag.CommandLine)
	pflag.Parse()

	if err := opts.Complete(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if err := opts.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(1)
	}

	ctx := ctrl.SetupSignalHandler()
	if err := server.NewRunner(opts).Run(ctx); err != nil {
		os.Exit(1)
	}
}
