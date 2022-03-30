package gloat

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMigrationReversible(t *testing.T) {
	m := Migration{}

	assert.False(t, m.Reversible())

	m.DownSQL = []byte("DROP TABLE users;")

	assert.True(t, m.Reversible())
}

func TestMigrationPersistable(t *testing.T) {
	m := Migration{}

	if m.Persistable() {
		t.Fatalf("Expected %v to not be persistable", m)
	}

	m.Path = "migrations/0001_something"
	assert.True(t, m.Persistable())
}

func TestMigrationFromPath(t *testing.T) {
	expectedPath := "testdata/migrations/20170329154959_introduce_domain_model"

	m, err := MigrationFromBytes(expectedPath, ioutil.ReadFile)
	assert.Nil(t, err)

	assert.Equal(t, int64(20170329154959), m.Version)
	assert.Equal(t, expectedPath, m.Path)
}

func TestMigrationsExcept(t *testing.T) {
	var migrations Migrations

	expectedPath := "testdata/migrations/20170329154959_introduce_domain_model"

	m, err := MigrationFromBytes(expectedPath, ioutil.ReadFile)
	assert.Nil(t, err)

	migrations = append(migrations, m)

	exceptedMigrations := migrations.Except(nil)
	assert.Nil(t, exceptedMigrations)

	exceptedMigrations = migrations.Except(Migrations{m})
	assert.Len(t, exceptedMigrations, 0)
}

func TestMigrationsIntersect(t *testing.T) {
	var migrations Migrations

	first := "testdata/migrations/20170329154959_introduce_domain_model"

	m1, err := MigrationFromBytes(first, ioutil.ReadFile)
	assert.Nil(t, err)

	second := "testdata/migrations/20180905150724_concurrent_migration"

	m2, err := MigrationFromBytes(second, ioutil.ReadFile)
	assert.Nil(t, err)

	migrations = append(migrations, m1)
	migrations = append(migrations, m2)

	result := migrations.Intersect(nil)
	assert.Len(t, result, 0)

	result = migrations.Intersect(Migrations{m1})
	assert.Len(t, result, 1)

	result = migrations.Intersect(Migrations{m1, m2})
	assert.Len(t, result, 2)
}

func TestMigrationsSort(t *testing.T) {
	var migrations Migrations

	first := "testdata/migrations/20170329154959_introduce_domain_model"

	m1, err := MigrationFromBytes(first, ioutil.ReadFile)
	assert.Nil(t, err)

	second := "testdata/migrations/20180905150724_concurrent_migration"

	m2, err := MigrationFromBytes(second, ioutil.ReadFile)
	assert.Nil(t, err)

	migrations = append(migrations, m2)
	migrations = append(migrations, m1)

	migrations.Sort()
	assert.Equal(t, migrations[0].Version, m1.Version)

	m1.AppliedAt = time.Now().UTC()
	m2.AppliedAt = m1.AppliedAt.Add(-1 * time.Hour)

	migrations.Sort()
	assert.Equal(t, migrations[0].Version, m2.Version)
}

func TestMigrationsReverseSort(t *testing.T) {
	var migrations Migrations

	first := "testdata/migrations/20170329154959_introduce_domain_model"

	m1, err := MigrationFromBytes(first, ioutil.ReadFile)
	assert.Nil(t, err)

	second := "testdata/migrations/20180905150724_concurrent_migration"

	m2, err := MigrationFromBytes(second, ioutil.ReadFile)
	assert.Nil(t, err)

	migrations = append(migrations, m2)
	migrations = append(migrations, m1)

	migrations.ReverseSort()
	assert.Equal(t, migrations[0].Version, m2.Version)

	m1.AppliedAt = time.Now().UTC()
	m2.AppliedAt = m1.AppliedAt.Add(time.Hour)

	migrations.ReverseSort()
	assert.Equal(t, migrations[0].Version, m2.Version)
}
