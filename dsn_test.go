package pgxrepmgr

import (
	"maps"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestParseDSN(t *testing.T) {
	for _, tc := range []struct {
		conninfo string
		want     map[string]string
	}{
		{"", map[string]string{}},
		{"host=a port=5432", map[string]string{"host": "a", "port": "5432"}},
		{"  host = a\tport=5432  ", map[string]string{"host": "a", "port": "5432"}},
		{"password=secret! host=a", map[string]string{"password": "secret!", "host": "a"}},
		{"host=/var/run/postgresql/", map[string]string{"host": "/var/run/postgresql/"}},
		{"password='a b' host=a", map[string]string{"password": "a b", "host": "a"}},
		{`password='it\'s \\ ok'`, map[string]string{"password": `it's \ ok`}},
		{`password=it\'s`, map[string]string{"password": "it's"}},
		{`password=a\b`, map[string]string{"password": `a\b`}},
		{"options='' host=a", map[string]string{"options": "", "host": "a"}},
		{"host=a host=b", map[string]string{"host": "b"}},
	} {
		got, err := ParseDSN(tc.conninfo)
		if err != nil {
			t.Errorf("ParseDSN(%q): %v", tc.conninfo, err)
		} else if !maps.Equal(got, tc.want) {
			t.Errorf("ParseDSN(%q) = %q, want %q", tc.conninfo, got, tc.want)
		}
	}
	for _, conninfo := range []string{"host", "host=a port", "=a", "password='secret", `password=secret\`} {
		if got, err := ParseDSN(conninfo); err == nil {
			t.Errorf("ParseDSN(%q) = %q, want an error", conninfo, got)
		}
	}
}

// pgx reads the values that BuildDSN writes as they were.
func TestBuildDSNQuotes(t *testing.T) {
	password := `a b'c\d!`
	conninfo := BuildDSN(map[string]string{"host": "a", "password": password, "options": ""})
	if want := `host=a options='' password='a b\'c\\d!'`; conninfo != want {
		t.Errorf("got %s, want %s", conninfo, want)
	}
	cfg, err := pgconn.ParseConfig(conninfo)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Password != password {
		t.Errorf("pgx read the password %q, want %q", cfg.Password, password)
	}
	parts, err := ParseDSN(conninfo)
	if err != nil {
		t.Fatal(err)
	}
	if parts["password"] != password {
		t.Errorf("ParseDSN read the password %q, want %q", parts["password"], password)
	}
}

func TestDSNWithDatabase(t *testing.T) {
	dsn, err := DSNWithDatabase("host=a dbname=repmgr", "app")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database != "app" {
		t.Errorf("DSNWithDatabase gave %q, pgx connects to database %q, want app", dsn, cfg.Database)
	}
}
