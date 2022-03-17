package gloat

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileSystemSourceCollect(t *testing.T) {
	td := "testdata/migrations"
	fs := NewFileSystemSource(td)

	migrations, err := fs.Collect()
	assert.Nil(t, err)

	m1, err := MigrationFromBytes(filepath.Join(td, "20170329154959_introduce_domain_model"), ioutil.ReadFile)
	assert.Nil(t, err)

	m2, err := MigrationFromBytes(filepath.Join(td, "20170511172647_irreversible_migration_brah"), ioutil.ReadFile)
	assert.Nil(t, err)

	m3, err := MigrationFromBytes(filepath.Join(td, "20180905150724_concurrent_migration"), ioutil.ReadFile)
	assert.Nil(t, err)

	m4, err := MigrationFromBytes(filepath.Join(td, "20180920181906_migration_with_an_error"), ioutil.ReadFile)
	assert.Nil(t, err)

	expectedMigrations := Migrations{m1, m2, m3, m4}
	assert.Equal(t, migrations, expectedMigrations)
}

func TestFileSystemSourceCollectEmpty(t *testing.T) {
	td := "testdata/no_migrations"
	fs := NewFileSystemSource(td)

	migrations, err := fs.Collect()
	assert.Nil(t, err)

	assert.Len(t, migrations, 0)
}

func TestAssetSourceDoesNotBreakOnIrreversibleMigrations(t *testing.T) {
	td := "testdata/migrations"
	fs := NewAssetSource(td, Asset, AssetDir)

	migrations, err := fs.Collect()
	assert.Nil(t, err)

	assert.Len(t, migrations, 4)
}
