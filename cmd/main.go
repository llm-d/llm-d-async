package main

import (
	"fmt"
	"os"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/pkg/server"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue/migrate"
	// Register the built-in request body-transform plugins.
	_ "github.com/llm-d/llm-d-async/pkg/asyncworker/transform/gcsmultipart"
	"github.com/spf13/pflag"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == migrate.Subcommand {
		ctx := logr.NewContext(ctrl.SetupSignalHandler(), zap.New())
		if err := migrate.RunCommand(ctx, os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
			os.Exit(1)
		}
		return
	}

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
