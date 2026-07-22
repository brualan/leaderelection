package postgres

import (
	"strings"
	"testing"
)

// The table name is the only part of these statements that cannot be a bind
// parameter, which makes it the only place a caller's string reaches the
// planner as syntax. It is validated rather than escaped, and these cases pin
// down that the validation is a whitelist.
func TestIdentifierValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		table string
		ok    bool
	}{
		{"plain", "leader_election", true},
		{"leading underscore", "_leases", true},
		{"digits and dollar", "leases_2$x", true},
		{"maximum length", strings.Repeat("a", 63), true},

		{"empty", "", false},
		{"too long for the server", strings.Repeat("a", 64), false},
		{"leading digit", "2leases", false},
		{"hyphen", "leader-election", false},
		{"space", "leader election", false},
		{"dot", "public.leases", false},
		{"embedded quote", `leases"`, false},
		{"statement terminator", "leases; drop table users", false},
		{"comment", "leases --", false},
		{"newline", "leases\nx", false},
		{"unicode", "выборы", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := qualify("", tc.table)
			if tc.ok && err != nil {
				t.Fatalf("qualify(%q) rejected a usable name: %v", tc.table, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("qualify(%q) accepted an unusable name", tc.table)
			}
		})
	}
}

func TestSchemaIdentifierIsValidatedToo(t *testing.T) {
	if _, err := qualify("public", "leases"); err != nil {
		t.Fatalf("qualify: %v", err)
	}
	if _, err := qualify(`bad"schema`, "leases"); err == nil {
		t.Fatal("qualify accepted an unusable schema name")
	}
	// A bad table is reported even when the schema is fine, and vice versa.
	if _, err := qualify("public", "bad table"); err == nil {
		t.Fatal("qualify accepted an unusable table name")
	}
}

func TestSchemaDDL(t *testing.T) {
	ddl, err := Schema("", DefaultTable)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	for _, want := range []string{
		`CREATE TABLE IF NOT EXISTS "leader_election"`,
		"name        text        PRIMARY KEY",
		"fence       bigint      NOT NULL",
		"expires_at  timestamptz NOT NULL",
		"fillfactor = 70",
	} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("DDL is missing %q:\n%s", want, ddl)
		}
	}
	if !strings.HasSuffix(ddl, ";\n") {
		t.Fatal("DDL is not terminated, so it cannot be pasted into a migration")
	}

	qualified, err := Schema("coord", "leases")
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	if !strings.Contains(qualified, `"coord"."leases"`) {
		t.Fatalf("DDL does not use the qualified name:\n%s", qualified)
	}

	if _, err := Schema("", "bad name"); err == nil {
		t.Fatal("Schema accepted an unusable table name")
	}
}

// Every statement must be a single one. A pooler in transaction mode gives
// back a different server connection after each transaction, so an operation
// split across two statements would be split across two backends.
func TestStatementsAreSingleAndSelfContained(t *testing.T) {
	for name, sql := range map[string]string{
		"acquire": acquireSQL,
		"renew":   renewSQL,
		"release": releaseSQL,
		"inspect": inspectSQL,
	} {
		t.Run(name, func(t *testing.T) {
			if strings.Contains(sql, ";") {
				t.Fatal("statement contains a terminator, so it may be more than one statement")
			}
			// One statement means one verb, and it has to be a data verb.
			switch verb := strings.Fields(strings.TrimSpace(sql))[0]; verb {
			case "INSERT", "UPDATE", "SELECT":
			default:
				t.Fatalf("statement starts with %q, want INSERT, UPDATE or SELECT", verb)
			}
			// Session state of any kind breaks under transaction pooling.
			for _, forbidden := range []string{
				"pg_advisory_lock", "pg_advisory_unlock", "LISTEN", "NOTIFY",
				"BEGIN", "COMMIT", "ROLLBACK", "PREPARE", "TEMPORARY", "FOR UPDATE",
			} {
				if strings.Contains(sql, forbidden) {
					t.Fatalf("statement uses %q, which does not survive a transaction pooler", forbidden)
				}
			}
			// now() rather than clock_timestamp(): every reference inside one
			// statement must return the same instant.
			if strings.Contains(sql, "clock_timestamp") {
				t.Fatal("statement uses clock_timestamp(), so its timestamps are not mutually consistent")
			}
			if !strings.Contains(sql, "now()") {
				t.Fatal("statement does not take its time from the server")
			}
		})
	}
}

// Every statement that returns a lease must return the server's current time
// alongside it, because the caller may only ever use the difference.
func TestLeaseProjectionsReturnServerTime(t *testing.T) {
	for name, sql := range map[string]string{
		"acquire": acquireSQL,
		"renew":   renewSQL,
		"inspect": inspectSQL,
	} {
		t.Run(name, func(t *testing.T) {
			const projection = "holder, fence, acquired_at, renewed_at, expires_at, payload, now()"
			if !strings.Contains(sql, projection) {
				t.Fatalf("statement does not return the standard projection %q", projection)
			}
		})
	}
}

// The guards are what stop a partitioned former leader from resurrecting its
// term. Losing any one of them loses the guarantee.
func TestRenewIsFullyGuarded(t *testing.T) {
	for _, guard := range []string{
		"name = $1",
		"holder = $2",
		"fence = $3",
		"expires_at > now()",
	} {
		if !strings.Contains(renewSQL, guard) {
			t.Fatalf("renew statement is missing the guard %q", guard)
		}
	}
}

// The fence must advance on exactly one condition: reclaiming an expired
// lease. Extending a term that never lapsed keeps its token.
func TestFenceAdvancesOnlyOnReclaim(t *testing.T) {
	const rule = "fence       = CASE WHEN l.expires_at <= now()\n" +
		"                       THEN l.fence + 1          ELSE l.fence       END"
	if !strings.Contains(acquireSQL, rule) {
		t.Fatalf("acquire statement does not advance the fence on reclaim only:\n%s", acquireSQL)
	}
	// The fence appears in the renew statement only as a guard, never as an
	// assignment: a renewal continues a term rather than starting one.
	set, _, ok := strings.Cut(renewSQL, "WHERE")
	if !ok {
		t.Fatal("renew statement has no WHERE clause")
	}
	if strings.Contains(set, "fence") {
		t.Fatalf("renew statement assigns the fence:\n%s", set)
	}
}
