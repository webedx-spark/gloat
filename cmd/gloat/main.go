package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/webedx-spark/gloat"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

const usage = `Usage gloat: [OPTION ...] [COMMAND ...]

Gloat is a Go SQL migration utility.

Commands:
  new                      Create a new migration folder
  up                       Apply new migrations
  down                     Revert the last applied migration
  to <version>             Migrate to a given version (down to).
  latest                   Latest migration in the source.
  current                  Latest Applied migration.
  present                  List all present versions.

Options:
  -quiet        Output only errors
  -src          The folder with migrations
                (default $DATABASE_SRC or database/migrations)
  -url          The database connection URL
                (default $DATABASE_URL)
  -help         Show this message
`

type arguments struct {
	url   string
	src   string
	quiet bool
	rest  []string
}

func main() {
	args := parseArguments()

	var cmdName string
	if len(args.rest) > 0 {
		cmdName = args.rest[0]
	}

	var err error
	switch cmdName {
	case "up":
		err = upCmd(args)
	case "down":
		err = downCmd(args)
	case "new":
		err = newCmd(args)
	case "to":
		err = migrateToCmd(args)
	case "latest":
		err = latestCmd(args)
	case "current":
		err = currentCmd(args)
	case "present":
		err = presentCmd(args)
	default:
		fmt.Fprintf(os.Stderr, usage)
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %+v\n", err)
		os.Exit(2)
	}
}

func upCmd(args arguments) error {
	gl, err := setupGloat(args)
	if err != nil {
		return err
	}

	migrations, err := gl.Unapplied()
	if err != nil {
		return err
	}

	appliedMigrations := map[int64]bool{}

	for _, migration := range migrations {
		printf(args, "Applying: %d...\n", migration.Version)

		if err := gl.Apply(migration); err != nil {
			return err
		}

		appliedMigrations[migration.Version] = true
	}

	if len(appliedMigrations) == 0 {
		printf(args, "No migrations to apply\n")
	}

	return nil
}

func latestCmd(args arguments) error {
	gl, err := setupGloat(args)
	if err != nil {
		return err
	}

	latest, err := gl.Latest()
	if err != nil {
		return err
	}

	if latest != nil {
		fmt.Printf("%d", latest.Version)
	}
	return nil
}

func presentCmd(args arguments) error {
	gl, err := setupGloat(args)
	if err != nil {
		return err
	}

	migrations, err := gl.Present()
	if err != nil {
		return err
	}

	migrations.Sort()

	for i, m := range migrations {
		fmt.Printf("%d", m.Version)
		if i != len(migrations)-1 {
			fmt.Print(",")
		}
	}

	return nil
}

func currentCmd(args arguments) error {
	gl, err := setupGloat(args)
	if err != nil {
		return err
	}

	current, err := gl.Current()
	if err != nil {
		return err
	}

	if current != nil {
		fmt.Printf("%d", current.Version)
	}
	return nil
}

func migrateToCmd(args arguments) error {
	gl, err := setupGloat(args)
	if err != nil {
		return err
	}
	if len(args.rest) < 2 {
		return errors.New("migrate to requires a version to migrate to")
	}

	version, err := strconv.ParseInt(args.rest[1], 10, 64)
	if err != nil {
		return err
	}

	migrations, err := gl.AppliedAfter(version)
	if err != nil {
		return err
	}

	for _, migration := range migrations {
		printf(args, "Reverting: %d...\n", migration.Version)

		if err := gl.Revert(migration); err != nil {
			return err
		}
	}

	return nil
}

func downCmd(args arguments) error {
	gl, err := setupGloat(args)
	if err != nil {
		return err
	}

	migration, err := gl.Current()
	if err != nil {
		return err
	}

	if migration == nil {
		printf(args, "No migrations to revert\n")
		return nil
	}

	printf(args, "Reverting: %d...\n", migration.Version)

	if err := gl.Revert(migration); err != nil {
		return err
	}

	return nil
}

func newCmd(args arguments) error {
	if _, err := os.Stat(args.src); os.IsNotExist(err) {
		return err
	}

	if len(args.rest) < 2 {
		return errors.New("new requires a migration name given as an argument")
	}

	migration := gloat.GenerateMigration(strings.Join(args.rest[1:], "_"))
	migrationDirectoryPath := filepath.Join(args.src, migration.Path)

	if err := os.MkdirAll(migrationDirectoryPath, 0755); err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(migrationDirectoryPath, "up.sql"))
	if err != nil {
		return err
	}
	f.Close()

	f, err = os.Create(filepath.Join(migrationDirectoryPath, "down.sql"))
	if err != nil {
		return err
	}
	f.Close()

	printf(args, "Created %s\n", migrationDirectoryPath)

	return nil
}

func parseArguments() arguments {
	var args arguments

	urlDefault := os.Getenv("DATABASE_URL")
	urlUsage := `database connection url`

	srcDefault := os.Getenv("DATABASE_SRC")
	if srcDefault == "" {
		srcDefault = "database/migrations"
	}
	srcUsage := `the folder with migrations`

	flag.StringVar(&args.url, "url", urlDefault, urlUsage)
	flag.StringVar(&args.src, "src", srcDefault, srcUsage)
	flag.BoolVar(&args.quiet, "quiet", false, "Output only errors")

	flag.Usage = func() { fmt.Fprintf(os.Stderr, usage) }

	flag.Parse()

	args.rest = flag.Args()

	return args
}

func setupGloat(args arguments) (*gloat.Gloat, error) {
	u, err := url.Parse(args.url)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(u.Scheme, args.url)
	if err != nil {
		return nil, err
	}

	store, err := databaseStoreFactory(u.Scheme, db)
	if err != nil {
		return nil, err
	}

	return &gloat.Gloat{
		Store:    store,
		Source:   gloat.NewFileSystemSource(args.src),
		Executor: gloat.NewSQLExecutor(db),
	}, nil
}

func databaseStoreFactory(driver string, db *sql.DB) (gloat.Store, error) {
	switch driver {
	case "postgres", "postgresql":
		return gloat.NewPostgreSQLStore(db), nil
	case "mysql":
		return gloat.NewMySQLStore(db), nil
	case "sqlite", "sqlite3":
		return gloat.NewMySQLStore(db), nil
	}

	return nil, errors.New("unsupported database driver " + driver)
}
func printf(args arguments, str string, subs ...interface{}) {
	if args.quiet != true {
		fmt.Printf(str, subs...)
	}
}
