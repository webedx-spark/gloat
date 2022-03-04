package gloat

// Store is an interface representing a place where the applied migrations are
// recorded.
type Store interface {
	Source

	Insert(*Migration, SQLExecer) error
	Remove(*Migration, SQLExecer) error
}

// DatabaseStore is a Store that keeps the applied migrations in a database
// table called schema_migrations. The table is automatically created if it
// does not exist.
type DatabaseStore struct {
	db SQLTransactor

	createTableStatement         string
	createIndexStatement         string
	insertMigrationStatement     string
	removeMigrationStatement     string
	selectAllMigrationsStatement string
}

// Insert records a migration version into the schema_migrations table.
func (s *DatabaseStore) Insert(migration *Migration, execer SQLExecer) error {
	if execer == nil {
		execer = s.db
	}

	if err := s.ensureSchemaTableExists(); err != nil {
		return err
	}

	_, err := execer.Exec(s.insertMigrationStatement, migration.Version, migration.VersionTag)
	return err
}

// Remove removes a migration version from the schema_migrations table.
func (s *DatabaseStore) Remove(migration *Migration, execer SQLExecer) error {
	if execer == nil {
		execer = s.db
	}

	if err := s.ensureSchemaTableExists(); err != nil {
		return err
	}

	_, err := execer.Exec(s.removeMigrationStatement, migration.Version)
	return err
}

// Collect builds a slice of migrations with the versions of the recorded
// applied migrations.
func (s *DatabaseStore) Collect() (migrations Migrations, err error) {
	if err = s.ensureSchemaTableExists(); err != nil {
		return
	}

	rows, err := s.db.Query(s.selectAllMigrationsStatement)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		migration := &Migration{}
		if err = rows.Scan(&migration.Version, &migration.VersionTag); err != nil {
			return
		}

		migrations = append(migrations, migration)
	}

	return
}

func (s *DatabaseStore) ensureSchemaTableExists() error {
	if _, err := s.db.Exec(s.createTableStatement); err != nil {
		return err
	}

	if _, err := s.db.Exec(s.createIndexStatement); err != nil {
		return err
	}

	return nil
}

// NewPostgreSQLStore creates a Store for PostgreSQL.
func NewPostgreSQLStore(db SQLTransactor) Store {
	return &DatabaseStore{
		db: db,
		createTableStatement: `
			CREATE TABLE IF NOT EXISTS schema_migrations (
				version BIGINT PRIMARY KEY NOT NULL,
				version_tag text
			)`,
		createIndexStatement: `
			CREATE INDEX IF NOT EXISTS schema_migrations_version_tag 
			ON schema_migrations (version_tag)
			`,
		insertMigrationStatement: `
			INSERT INTO schema_migrations (version, version_tag)
			VALUES ($1, $2)`,
		removeMigrationStatement: `
			DELETE FROM schema_migrations
			WHERE version=$1`,
		selectAllMigrationsStatement: `
			SELECT version, version_tag
			FROM schema_migrations
			ORDER BY version DESC`,
	}
}

// NewMySQLStore creates a Store for MySQL.
func NewMySQLStore(db SQLTransactor) Store {
	return &DatabaseStore{
		db: db,
		createTableStatement: `
			CREATE TABLE IF NOT EXISTS schema_migrations (
				version BIGINT PRIMARY KEY NOT NULL,
				version_tag text
			)`,
		createIndexStatement: `
			CREATE INDEX IF NOT EXISTS schema_migrations_version_tag 
			ON schema_migrations (version_tag)
			`,
		insertMigrationStatement: `
			INSERT INTO schema_migrations (version, version_tag)
			VALUES (?, ?)`,
		removeMigrationStatement: `
			DELETE FROM schema_migrations
			WHERE version=?`,
		selectAllMigrationsStatement: `
			SELECT version, version_tag
			FROM schema_migrations
			ORDER BY version DESC`,
	}
}

// NewSQLite3Store creates a Store for SQLite3.
func NewSQLite3Store(db SQLTransactor) Store {
	return &DatabaseStore{
		db: db,
		createTableStatement: `
			CREATE TABLE IF NOT EXISTS schema_migrations (
				version BIGINT PRIMARY KEY NOT NULL
				version_tag text
			)`,
		insertMigrationStatement: `
			INSERT INTO schema_migrations (version, version_tag)
			VALUES (?, ?)`,
		createIndexStatement: `
			CREATE INDEX IF NOT EXISTS schema_migrations_version_tag 
			ON schema_migrations (version_tag)
			`,
		removeMigrationStatement: `
			DELETE FROM schema_migrations
			WHERE version=?`,
		selectAllMigrationsStatement: `
			SELECT version, version_tag
			FROM schema_migrations
			ORDER BY version DESC`,
	}
}
