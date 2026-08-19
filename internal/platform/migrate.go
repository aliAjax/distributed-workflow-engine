package platform

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type Migrator struct {
	db  *sql.DB
	dir string
}

func NewMigrator(db *sql.DB, dir string) *Migrator {
	return &Migrator{db: db, dir: dir}
}

func (m *Migrator) Up(ctx context.Context) error {
	if m.dir == "" {
		m.dir = "migrations"
	}
	if err := m.ensureSchemaVersion(ctx); err != nil {
		return err
	}
	files, err := m.upFiles()
	if err != nil {
		return err
	}
	files = uniqueMigrations(files)
	for _, file := range files {
		applied, err := m.isApplied(ctx, file)
		if err != nil {
			return err
		}
		if applied {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", file, err)
		}
		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", file, err)
		}
		if _, err := tx.ExecContext(ctx, string(raw)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", file, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES($1)`, filepath.Base(file)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %s: %w", file, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", file, err)
		}
	}
	return nil
}

func (m *Migrator) Down(ctx context.Context) error {
	if m.dir == "" {
		m.dir = "migrations"
	}
	if err := m.ensureSchemaVersion(ctx); err != nil {
		return err
	}
	files, err := m.downFiles()
	if err != nil {
		return err
	}
	files = uniqueMigrations(files)
	for i := len(files) - 1; i >= 0; i-- {
		file := files[i]
		raw, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read down migration %s: %w", file, err)
		}
		tx, err := m.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin down migration %s: %w", file, err)
		}
		if _, err := tx.ExecContext(ctx, string(raw)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply down migration %s: %w", file, err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM schema_migrations WHERE version=$1`, filepath.Base(file)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("remove migration record %s: %w", file, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit down migration %s: %w", file, err)
		}
	}
	return nil
}

func (m *Migrator) ensureSchemaVersion(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`)
	return err
}

func (m *Migrator) isApplied(ctx context.Context, file string) (bool, error) {
	var exists bool
	err := m.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, filepath.Base(file)).Scan(&exists)
	return exists, err
}

func (m *Migrator) upFiles() ([]string, error) {
	return filepath.Glob(filepath.Join(m.dir, "*.up.sql"))
}

func (m *Migrator) downFiles() ([]string, error) {
	files, err := filepath.Glob(filepath.Join(m.dir, "*.down.sql"))
	sort.Sort(sort.Reverse(sort.StringSlice(files)))
	return files, err
}

func (m *Migrator) dirOrDefault() string {
	return m.dir
}

func uniqueMigrations(files []string) []string {
	seen := make(map[string]bool, len(files))
	out := make([]string, 0, len(files))
	for _, file := range files {
		base := filepath.Base(file)
		if seen[base] {
			continue
		}
		seen[base] = true
		out = append(out, file)
	}
	return out
}

func NormalizeDSN(dsn string) string {
	return dsn
}
