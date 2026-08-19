package platform

import "testing"

func TestPlatformJSONMapNilScan(t *testing.T) {
	var value JSONMap
	if err := value.Scan(nil); err != nil {
		t.Fatalf("scan nil JSONMap: %v", err)
	}
	if value == nil {
		t.Fatal("JSONMap should be initialized after scanning nil")
	}
	value["key"] = "value"
}

func TestPlatformJSONStringMapNilScan(t *testing.T) {
	var value JSONStringMap
	if err := value.Scan(nil); err != nil {
		t.Fatalf("scan nil JSONStringMap: %v", err)
	}
	if value == nil {
		t.Fatal("JSONStringMap should be initialized after scanning nil")
	}
	value["key"] = "value"
}

func TestPlatformStringSliceNilScan(t *testing.T) {
	var value StringSlice
	if err := value.Scan(nil); err != nil {
		t.Fatalf("scan nil StringSlice: %v", err)
	}
	if value == nil {
		t.Fatal("StringSlice should be initialized after scanning nil")
	}
	value = append(value, "x")
}

func TestPlatformStringSliceNilValue(t *testing.T) {
	var value StringSlice
	raw, err := value.Value()
	if err != nil {
		t.Fatalf("value: %v", err)
	}
	if string(raw.([]byte)) != "[]" {
		t.Fatalf("expected empty JSON array, got %s", raw)
	}
}

func TestPlatformJSONMapNilValue(t *testing.T) {
	var value JSONMap
	raw, err := value.Value()
	if err != nil {
		t.Fatalf("value: %v", err)
	}
	if string(raw.([]byte)) != "{}" {
		t.Fatalf("expected empty JSON object, got %s", raw)
	}
}

func TestPlatformJSONStringMapNilValue(t *testing.T) {
	var value JSONStringMap
	raw, err := value.Value()
	if err != nil {
		t.Fatalf("value: %v", err)
	}
	if string(raw.([]byte)) != "{}" {
		t.Fatalf("expected empty JSON object, got %s", raw)
	}
}

func TestPlatformMigratorDirOrDefault(t *testing.T) {
	migrator := &Migrator{}
	if got := migrator.dirOrDefault(); got != "migrations" {
		t.Fatalf("expected default migrations dir, got %q", got)
	}
}

func TestPlatformNormalizeDSNAddsSSLMode(t *testing.T) {
	if got := NormalizeDSN("postgres://localhost/db"); got != "postgres://localhost/db?sslmode=disable" {
		t.Fatalf("unexpected normalized dsn: %s", got)
	}
}

func TestPlatformUniqueMigrationsNoPanic(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("uniqueMigrations panicked: %v", recovered)
		}
	}()
	got := uniqueMigrations([]string{"001_init.up.sql", "001_init.up.sql"})
	if len(got) != 1 {
		t.Fatalf("expected one unique migration, got %d", len(got))
	}
}
