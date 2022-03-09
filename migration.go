package gloat

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	now = time.Now().UTC()

	ErrNotFound      = errors.New("version not found")
	nameNormalizerRe = regexp.MustCompile(`([a-z])([A-Z])`)
	versionFormat    = "20060102150405"
)

// Migration holds all the relevant information for a migration. The content of
// the UP side, the DOWN side, a path and version. The version is used to
// determine the order of which the migrations would be executed. The path is
// the name in a store.
type Migration struct {
	UpSQL     []byte
	DownSQL   []byte
	Path      string
	Version   int64
	Options   MigrationOptions
	AppliedAt time.Time
}

// Reversible returns true if the migration DownSQL content is present. E.g. if
// both of the directions are present in the migration folder.
func (m *Migration) Reversible() bool {
	return len(m.DownSQL) != 0
}

// Persistable is any migration with non blank Path.
func (m *Migration) Persistable() bool {
	return m.Path != ""
}

// GenerateMigration generates a new blank migration with blank UP and DOWN
// content defined from user entered content.
func GenerateMigration(str string) *Migration {
	version := generateVersion()
	path := generateMigrationPath(version, str)

	return &Migration{
		Path:    path,
		Version: version,
		Options: DefaultMigrationOptions(),
	}
}

// MigrationFromBytes builds a Migration struct from a path and a
// function. Functions like ioutil.ReadFile, go-bindata's Asset have
// the very same signature, so you can use them here.
func MigrationFromBytes(path string, read func(string) ([]byte, error)) (*Migration, error) {
	version, err := versionFromPath(path)
	if err != nil {
		return nil, err
	}

	upSQL, err := read(filepath.Join(path, "up.sql"))
	if err != nil {
		return nil, err
	}

	// This may be an error from the OS or the go-bindata generated Asset
	// function ("Asset %s can't read by error: %v"). Just ignore it, as we can
	// have embedded irreversible migrations.
	downSQL, _ := read(filepath.Join(path, "down.sql"))

	optionsJSON, err := read(filepath.Join(path, "options.json"))
	if err != nil {
		optionsJSON = nil
	}

	options, err := parseMigrationOptions(optionsJSON)
	if err != nil {
		return nil, err
	}

	return &Migration{
		UpSQL:     upSQL,
		DownSQL:   downSQL,
		Path:      path,
		Version:   version,
		Options:   options,
		AppliedAt: time.Time{0},
	}, nil
}

func generateMigrationPath(version int64, str string) string {
	name := strings.ToLower(nameNormalizerRe.ReplaceAllString(str, "${1}_${2}"))
	return fmt.Sprintf("%d_%s", version, name)
}

func generateVersion() int64 {
	version, _ := strconv.ParseInt(now.Format(versionFormat), 10, 64)
	return version
}

func versionFromPath(path string) (int64, error) {
	parts := strings.SplitN(filepath.Base(path), "_", 2)
	if len(parts) == 0 {
		return 0, fmt.Errorf("cannot extract version from %s", path)
	}

	return strconv.ParseInt(parts[0], 10, 64)
}

// Migrations is a slice of Migration pointers.
type Migrations []*Migration

// Except selects migrations that does not exist in the current ones.
func (m Migrations) Except(migrations Migrations) (excepted Migrations) {
	// Mark the current transactions.
	current := make(map[int64]time.Time)
	for _, migration := range m {
		current[migration.Version] = migration.AppliedAt
	}

	// Mark the ones in the migrations set, which we do have to get.
	new := make(map[int64]time.Time)
	for _, migration := range migrations {
		new[migration.Version] = migration.AppliedAt
	}

	for _, migration := range migrations {
		_, will := new[migration.Version]
		_, has := current[migration.Version]
		if will && !has {
			excepted = append(excepted, migration)
		}
	}

	return
}

// Intersect selects migrations that does exist in the current ones.
func (m Migrations) Intersect(migrations Migrations) (intersect Migrations) {
	// Mark the current transactions.
	store := make(map[int64]time.Time)
	for _, migration := range m {
		store[migration.Version] = migration.AppliedAt
	}

	// Mark the ones in the migrations set, which we do have to get.
	source := make(map[int64]time.Time)
	for _, migration := range migrations {
		source[migration.Version] = migration.AppliedAt
	}

	for _, migration := range migrations {
		_, will := source[migration.Version]
		appliedAt, has := store[migration.Version]
		if will && has {
			migration.AppliedAt = appliedAt
			intersect = append(intersect, migration)
		}
	}

	return
}

// Implementation for the sort.Sort interface.
func (m Migrations) Len() int { return len(m) }

// Less will sort by AppliedAt. If equal will sort by Version
func (m Migrations) Less(i, j int) bool {
	if m[i].AppliedAt.Before(m[j].AppliedAt) {
		return true
	}
	if m[i].AppliedAt.After(m[j].AppliedAt) {
		return false
	}
	return m[i].Version < m[j].Version
}

func (m Migrations) Swap(i, j int) { m[i], m[j] = m[j], m[i] }

// Sort is a convenience sorting method.
func (m Migrations) Sort() { sort.Sort(m) }

// ReverseSort is a convenience sorting method.
func (m Migrations) ReverseSort() { sort.Sort(sort.Reverse(m)) }

// Current returns the latest applied migration. Can be nil, if the migrations
// are empty.
func (m Migrations) Current() *Migration {
	m.Sort()

	if len(m) == 0 {
		return nil
	}

	return m[len(m)-1]
}

// AppliedAfter selects the applied migrations from a Store after a given version.
func AppliedAfter(store Source, source Source, version int64) (Migrations, error) {
	var appliedAfter Migrations
	appliedMigrations, err := store.Collect()
	if err != nil {
		return nil, err
	}

	found := false
	for _, migration := range appliedMigrations {
		if migration.Version == version {
			found = true
			break
		}
		appliedAfter = append(appliedAfter, migration)
	}

	if !found {
		return nil, ErrNotFound
	}

	appliedAfter.ReverseSort()

	availableMigrations, err := source.Collect()
	if err != nil {
		return nil, err
	}

	intersect := appliedAfter.Intersect(availableMigrations)
	intersect.ReverseSort()
	return intersect, nil
}

// UnappliedMigrations selects the unapplied migrations from a Source. For a
// migration to be unapplied it should not be present in the Store.
func UnappliedMigrations(store, source Source) (Migrations, error) {
	appliedMigrations, err := store.Collect()
	if err != nil {
		return nil, err
	}

	incomingMigrations, err := source.Collect()
	if err != nil {
		return nil, err
	}

	unappliedMigrations := appliedMigrations.Except(incomingMigrations)
	unappliedMigrations.Sort()

	return unappliedMigrations, nil
}
