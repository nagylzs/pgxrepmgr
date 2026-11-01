package pgxrepmgr

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// spaces are the characters that separate the key=value pairs of a connection string.
const spaces = " \t\n\v\f\r"

// ParseDSN parses a key=value PostgreSQL connection string into a map, for example
// "host=db.example.com port=5432 password='my secret'". A value can be in single quotes, and a
// backslash escapes a quote or a backslash, the same way pgx parses them. URLs (postgres://...) are
// not supported. The error doesn't contain the connection string, because it may contain a password.
func ParseDSN(dsn string) (map[string]string, error) {
	parts := make(map[string]string)
	s := dsn
	for {
		s = strings.TrimLeft(s, spaces)
		if s == "" {
			return parts, nil
		}
		end := strings.IndexAny(s, "="+spaces)
		if end < 0 {
			end = len(s)
		}
		key := s[:end]
		s = strings.TrimLeft(s[end:], spaces)
		if key == "" {
			return nil, fmt.Errorf("invalid connection string: a key is missing before \"=\"")
		}
		if !strings.HasPrefix(s, "=") {
			return nil, fmt.Errorf("invalid connection string: \"=\" is missing after %q", key)
		}
		s = strings.TrimLeft(s[1:], spaces)
		quoted := strings.HasPrefix(s, "'")
		if quoted {
			s = s[1:]
		}
		var value strings.Builder
		for {
			if s == "" {
				if quoted {
					return nil, fmt.Errorf("invalid connection string: the quoted value of %q is not terminated", key)
				}
				break
			}
			ch := s[0]
			if quoted && ch == '\'' {
				s = s[1:]
				break
			}
			if !quoted && strings.IndexByte(spaces, ch) >= 0 {
				break
			}
			if ch == '\\' {
				if len(s) == 1 {
					return nil, fmt.Errorf("invalid connection string: the value of %q ends with a backslash", key)
				}
				// Like pgx, keep the backslash if it doesn't escape a quote or a backslash
				if s[1] == '\\' || s[1] == '\'' {
					value.WriteByte(s[1])
				} else {
					value.WriteString(s[:2])
				}
				s = s[2:]
				continue
			}
			value.WriteByte(ch)
			s = s[1:]
		}
		parts[key] = value.String()
	}
}

// BuildDSN builds a key=value PostgreSQL connection string from a map. It sorts the keys, so the
// same map always gives the same string, and it quotes the values that need it.
func BuildDSN(parts map[string]string) string {
	keys := make([]string, 0, len(parts))
	for k := range parts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	items := make([]string, 0, len(keys))
	for _, name := range keys {
		items = append(items, name+"="+quoteValue(parts[name]))
	}
	return strings.Join(items, " ")
}

// quoteValue returns v as it can be written in a connection string: in quotes if it is empty, or
// contains a space, a quote or a backslash.
func quoteValue(v string) string {
	if v != "" && !strings.ContainsAny(v, spaces+`'\`) {
		return v
	}
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
}

// ReplaceInDSN sets the keys of changes in a PostgreSQL connection string. The result is normalized
// (see BuildDSN).
func ReplaceInDSN(dsn string, changes map[string]string) (string, error) {
	parts, err := ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	for name, value := range changes {
		parts[name] = value
	}
	return BuildDSN(parts), nil
}

// DSNWithHostPort replaces the host and the port in a PostgreSQL connection string. The result is
// normalized (see BuildDSN).
func DSNWithHostPort(dsn string, host string, port uint16) (string, error) {
	return ReplaceInDSN(dsn, map[string]string{
		"host": host,
		"port": strconv.Itoa(int(port)),
	})
}

// DSNWithDatabase replaces the database name in a PostgreSQL connection string. The result is
// normalized (see BuildDSN).
func DSNWithDatabase(dsn string, database string) (string, error) {
	return ReplaceInDSN(dsn, map[string]string{
		"dbname": database,
	})
}

// RemoveSensitiveFromDSN removes the user and the password from a PostgreSQL connection string, for
// example to log it.
func RemoveSensitiveFromDSN(dsn string) (string, error) {
	parts, err := ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	delete(parts, "user")
	delete(parts, "password")
	return BuildDSN(parts), nil
}
