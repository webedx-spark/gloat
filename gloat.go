package gloat

import (
	"database/sql"
)

// Gloat glues all the components needed to apply and revert
// migrations.
type Gloat struct {
	// Source is an incoming source of migrations. It can be File System or
	// embedded migrations with go-bindata, etc.
	Source Source

	// Store is the place where we store the applied migration versions. Can
	// be one of the builtin database store or custom file storage, etc.
	Store Store

	// Executor applies migrations and marks the newly applied migration
	// versions in the Store.
	Executor Executor
}

// AppliedAfter returns migrations that were applied after a given version tag
func (c *Gloat) AppliedAfter(version int64) (Migrations, error) {
	return AppliedAfter(c.Store, c.Source, version)
}

// Present returns all available migrations.
func (c *Gloat) Present() (Migrations, error) {
	migrations, err := c.Source.Collect()
	if err != nil {
		return err
	}
	migrations.Sort()
	return migrations, nil
}

// Unapplied returns the unapplied migrations in the current gloat.
func (c *Gloat) Unapplied() (Migrations, error) {
	return UnappliedMigrations(c.Store, c.Source)
}

// Latest returns the latest migration in the source.
func (c *Gloat) Latest() (*Migration, error) {
	availableMigrations, err := c.Source.Collect()
	if err != nil {
		return nil, err
	}

	latest := availableMigrations.Current()
	return latest, nil
}

// Current returns the latest applied migration. Even if no error is returned,
// the current migration can be nil.
//
// This is the case when the last applied migration is no longer available from
// the source or there are no migrations to begin with.
func (c *Gloat) Current() (*Migration, error) {
	appliedMigrations, err := c.Store.Collect()
	if err != nil {
		return nil, err
	}

	currentMigration := appliedMigrations.Current()
	if currentMigration == nil {
		return nil, nil
	}

	availableMigrations, err := c.Source.Collect()
	if err != nil {
		return nil, err
	}

	for i := len(availableMigrations) - 1; i >= 0; i-- {
		migration := availableMigrations[i]

		if migration.Version == currentMigration.Version {
			migration.AppliedAt = currentMigration.AppliedAt
			return migration, nil
		}
	}

	return nil, nil
}

// Apply applies a migration.
func (c *Gloat) Apply(migration *Migration) error {
	return c.Executor.Up(migration, c.Store)
}

// Revert rollbacks a migration.
func (c *Gloat) Revert(migration *Migration) error {
	return c.Executor.Down(migration, c.Store)
}

// SQLExecer is an interface compatible with sql.Tx.Exec. Can be passed as
// nil on non-SQL stores.
type SQLExecer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

// SQLTransactor is usually satisfied by *sql.DB, but can be used by wrappers
// around it.
type SQLTransactor interface {
	SQLExecer

	Begin() (*sql.Tx, error)
}
