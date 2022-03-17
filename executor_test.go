package gloat

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSQLExecutor_Up(t *testing.T) {
	td := filepath.Join(dbSrc, "20170329154959_introduce_domain_model")

	exe := NewSQLExecutor(db)

	migration, err := MigrationFromBytes(td, ioutil.ReadFile)
	assert.Nil(t, err)

	cleanState(func() {
		err := exe.Up(migration, new(testingStore))
		assert.Nil(t, err)

		_, err = db.Exec(`SELECT id FROM users LIMIT 1`)
		assert.Nil(t, err)
	})
}

func TestSQLExecutor_Up_Broken(t *testing.T) {
	td := filepath.Join(dbSrc, "20180920181906_migration_with_an_error")

	exe := NewSQLExecutor(db)

	migration, err := MigrationFromBytes(td, ioutil.ReadFile)
	assert.Nil(t, err)

	cleanState(func() {
		err := exe.Up(migration, new(testingStore))
		assert.Error(t, err)
	})
}

func TestSQLExecutor_Down(t *testing.T) {
	td := filepath.Join(dbSrc, "20170329154959_introduce_domain_model")

	exe := NewSQLExecutor(db)

	migration, err := MigrationFromBytes(td, ioutil.ReadFile)
	assert.Nil(t, err)

	cleanState(func() {
		exe.Up(migration, new(testingStore))
		assert.Nil(t, err)

		err = exe.Down(migration, new(testingStore))
		assert.Nil(t, err)

		_, err := db.Exec(`SELECT id FROM users LIMIT 1`)
		assert.NotNil(t, err)
	})
}

func TestSQLExecutor_Down_Broken(t *testing.T) {
	td := filepath.Join(dbSrc, "20180920181906_migration_with_an_error")

	exe := NewSQLExecutor(db)

	migration, err := MigrationFromBytes(td, ioutil.ReadFile)
	assert.Nil(t, err)

	cleanState(func() {
		err := exe.Down(migration, new(testingStore))
		assert.Error(t, err)
	})
}
