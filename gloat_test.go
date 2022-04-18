package gloat

import (
	"database/sql"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"

	// Needed to establish database connections during testing.
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	gl Gloat

	db          *sql.DB
	dbURL       string
	dbSrc       string
	dbSrcBroken string
	dbDriver    string
)

type testingStore struct{ applied Migrations }

func (s *testingStore) Collect() (Migrations, error)                   { return s.applied, nil }
func (s *testingStore) Insert(migration *Migration, _ SQLExecer) error { return nil }
func (s *testingStore) Remove(migration *Migration, _ SQLExecer) error { return nil }

type testingExecutor struct{}

func (e *testingExecutor) Up(*Migration, Store) error   { return nil }
func (e *testingExecutor) Down(*Migration, Store) error { return nil }

type stubbedExecutor struct {
	up   func(*Migration, Store) error
	down func(*Migration, Store) error
}

func (e *stubbedExecutor) Up(m *Migration, s Store) error {
	if e.up != nil {
		return e.up(m, s)
	}

	return nil
}

func (e *stubbedExecutor) Down(m *Migration, s Store) error {
	if e.down != nil {
		e.down(m, s)
	}

	return nil
}

func cleanState(fn func()) error {
	_, err := db.Exec(`
		DROP TABLE IF EXISTS schema_migrations;	
		DROP TABLE IF EXISTS users;	
	`)

	if err != nil {
		return err
	}

	fn()

	return nil
}

func databaseStoreFactory(driver string, db *sql.DB) (Store, error) {
	switch driver {
	case "postgres", "postgresql":
		return NewPostgreSQLStore(db), nil
	case "mysql":
		return NewMySQLStore(db), nil
	case "sqlite", "sqlite3":
		return NewMySQLStore(db), nil
	}

	return nil, errors.New("unsupported database driver " + driver)
}

func TestUnapplied(t *testing.T) {
	gl.Store = &testingStore{applied: Migrations{}}

	migrations, err := gl.Unapplied()
	assert.Nil(t, err)
	assert.Len(t, migrations, 4)

	assert.Equal(t, int64(20170329154959), migrations[0].Version)
}

func TestUnapplied_Empty(t *testing.T) {
	gl.Store = &testingStore{
		applied: Migrations{
			&Migration{Version: 20170329154959},
			&Migration{Version: 20170511172647},
			&Migration{Version: 20180905150724},
			&Migration{Version: 20180920181906},
		},
	}

	migrations, err := gl.Unapplied()
	assert.Nil(t, err)

	assert.Len(t, migrations, 0)
}

func TestUnapplied_MissingInSource(t *testing.T) {
	gl.Store = &testingStore{
		applied: Migrations{
			&Migration{Version: 20170329154959},
			&Migration{Version: 20170511172647},
			&Migration{Version: 20180905150724},
			&Migration{Version: 20180920181906},
		},
	}

	prevSource := gl.Source
	defer func() { gl.Source = prevSource }()
	gl.Source = &testingStore{}

	migrations, err := gl.Unapplied()
	assert.Nil(t, err)

	assert.Len(t, migrations, 0)
}

func TestCurrent(t *testing.T) {
	gl.Store = &testingStore{
		applied: Migrations{
			&Migration{Version: 20170329154959},
		},
	}

	migration, err := gl.Current()
	assert.Nil(t, err)

	assert.NotNil(t, migration)
	assert.Equal(t, int64(20170329154959), migration.Version)
}

func TestAppliedAfter(t *testing.T) {
	gl.Source = &testingStore{
		applied: Migrations{
			&Migration{Version: 20190329154959, DownSQL: []byte("some sql here")},
			&Migration{Version: 20180329154959},
			&Migration{Version: 20170329154959},
		},
	}
	gl.Store = &testingStore{
		applied: Migrations{
			&Migration{Version: 20190329154959},
			&Migration{Version: 20180329154959},
			&Migration{Version: 20170329154959},
		},
	}

	migrations, err := gl.AppliedAfter(20170329154959)
	assert.Nil(t, err)

	assert.NotNil(t, migrations)
	require.Len(t, migrations, 2)
	assert.Equal(t, int64(20190329154959), migrations[0].Version)
	assert.True(t, migrations[0].Reversible())
}

func TestLatest(t *testing.T) {
	gl.Source = &testingStore{
		applied: Migrations{
			&Migration{Version: 20190329154959},
			&Migration{Version: 20180329154959},
			&Migration{Version: 20170329154959},
		},
	}

	migration, err := gl.Latest()
	assert.Nil(t, err)

	assert.NotNil(t, migration)
	assert.Equal(t, int64(20190329154959), migration.Version)
}

func TestCurrent_Nil(t *testing.T) {
	gl.Store = &testingStore{}

	migration, err := gl.Current()
	assert.Nil(t, err)

	assert.Nil(t, migration)
}

func TestApply(t *testing.T) {
	called := false

	m := &Migration{}
	gl.Store = &testingStore{}
	gl.Executor = &stubbedExecutor{
		up: func(*Migration, Store) error {
			called = true
			return nil
		},
	}

	gl.Apply(m)

	assert.True(t, called)
	assert.NotEmpty(t, m.AppliedAt)
}

func TestRevert(t *testing.T) {
	called := false

	gl.Store = &testingStore{}
	gl.Executor = &stubbedExecutor{
		down: func(*Migration, Store) error {
			called = true
			return nil
		},
	}

	gl.Revert(nil)

	assert.True(t, called)
}

func init() {
	gl = Gloat{
		Source:   NewFileSystemSource("testdata/migrations"),
		Executor: &testingExecutor{},
	}

	dbURL = os.Getenv("DATABASE_URL")
	dbSrc = os.Getenv("DATABASE_SRC")

	{
		u, err := url.Parse(dbURL)
		if err != nil {
			panic(err)
		}

		dbDriver = u.Scheme
	}

	// Do a bit of post-processing so we can connect to non-postgres databases.
	if dbDriver != "postgres" {
		parts := strings.SplitN(dbURL, "://", 2)

		if len(parts) != 2 {
			panic("Cannot split " + dbURL + " into parts")
		}

		dbURL = parts[1]
	}

	{
		var err error

		db, err = sql.Open(dbDriver, dbURL)
		if err != nil {
			panic(err)
		}

		if err := db.Ping(); err != nil {
			panic(err)
		}
	}
}
