package migrate

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"

	"github.com/go-logr/logr"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Subcommand is the argv[1] value that dispatches a binary into RunCommand.
const Subcommand = "migrate"

const urlEnv = "SQL_URL"

// RunCommand is the `migrate` subcommand. It connects to Postgres, applies
// pending migrations, and returns. The connection URL comes from
// --postgresql-url or, when unset, SQL_URL.
func RunCommand(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet(Subcommand, flag.ContinueOnError)
	url := fs.String("postgresql-url", "", "Postgres connection URL (default: $"+urlEnv+")")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if *url == "" {
		*url = os.Getenv(urlEnv)
	}
	if *url == "" {
		return fmt.Errorf("no Postgres URL: pass --postgresql-url or set %s", urlEnv)
	}

	db, err := sql.Open("pgx", *url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = db.Close() }()

	logger := logr.FromContextOrDiscard(ctx)
	logger.Info("applying sql transport schema migrations")

	applied, err := Run(ctx, db)
	if err != nil {
		return err
	}
	for _, m := range applied {
		logger.Info("applied migration", "version", m.Version, "name", m.Name)
	}

	version, err := LatestVersion()
	if err != nil {
		return err
	}
	logger.Info("sql transport schema migrations complete", "applied", len(applied), "version", version)
	return nil
}
