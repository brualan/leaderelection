package mysql

import (
	"strings"
	"testing"
)

// The table name is the only part of these statements that cannot be a bind
// parameter, which makes it the only place a caller's string reaches the parser
// as syntax. It is validated rather than escaped, and these cases pin down that
// the validation is a whitelist.
func TestIdentifierValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		table string
		ok    bool
	}{
		{"plain", "leader_election", true},
		{"leading underscore", "_leases", true},
		{"digits and dollar", "leases_2$x", true},
		{"maximum length", strings.Repeat("a", 64), true},

		{"empty", "", false},
		{"too long for the server", strings.Repeat("a", 65), false},
		{"leading digit", "2leases", false},
		{"hyphen", "leader-election", false},
		{"space", "leader election", false},
		{"dot", "app.leases", false},
		{"embedded backtick", "leases`", false},
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

func TestDatabaseIdentifierIsValidatedToo(t *testing.T) {
	if _, err := qualify("app", "leases"); err != nil {
		t.Fatalf("qualify: %v", err)
	}
	if _, err := qualify("bad`db", "leases"); err == nil {
		t.Fatal("qualify accepted an unusable database name")
	}
	// A bad table is reported even when the database is fine, and vice versa.
	if _, err := qualify("app", "bad table"); err == nil {
		t.Fatal("qualify accepted an unusable table name")
	}
}

func TestSchemaDDL(t *testing.T) {
	ddl, err := Schema("", DefaultTable)
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `leader_election`",
		"name        VARBINARY(255) NOT NULL",
		"holder      VARBINARY(255) NOT NULL",
		"fence       BIGINT         NOT NULL",
		"expires_at  DATETIME(6)    NOT NULL",
		"PRIMARY KEY (name)",
		"ENGINE=InnoDB",
	} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("DDL is missing %q:\n%s", want, ddl)
		}
	}
	if !strings.HasSuffix(ddl, ";\n") {
		t.Fatal("DDL is not terminated, so it cannot be pasted into a migration")
	}

	qualified, err := Schema("app", "leases")
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	if !strings.Contains(qualified, "`app`.`leases`") {
		t.Fatalf("DDL does not use the qualified name:\n%s", qualified)
	}

	if _, err := Schema("", "bad name"); err == nil {
		t.Fatal("Schema accepted an unusable table name")
	}
}

// The identity and the election name are compared byte for byte, and a text
// column under MySQL's default collation would not be. Two identities that
// differ only in case, or in a trailing space, would then be the same
// participant to every guard in this package.
func TestIdentityColumnsAreBinary(t *testing.T) {
	for _, column := range []string{"name", "holder"} {
		if !strings.Contains(schemaSQL, column+"        VARBINARY") &&
			!strings.Contains(schemaSQL, column+"      VARBINARY") {
			t.Fatalf("column %q is not VARBINARY:\n%s", column, schemaSQL)
		}
	}
	// A collation named anywhere would mean somebody moved back to a text
	// column and reintroduced the problem.
	if strings.Contains(strings.ToUpper(schemaSQL), "COLLATE") {
		t.Fatalf("the schema names a collation, so it is comparing text:\n%s", schemaSQL)
	}
}

// Every statement must be a single one. A pooler is free to hand the next
// statement to a different backend, so an operation split across two statements
// would be split across two connections -- and the write, at least, must never
// be.
func TestStatementsAreSingleAndSelfContained(t *testing.T) {
	for name, sql := range statements() {
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
			// Session state of any kind breaks under a pooler, and a user
			// variable is the MySQL-flavoured way to reach for it.
			for _, forbidden := range []string{
				"GET_LOCK", "RELEASE_LOCK", "@", "SET SESSION", "SET GLOBAL", "START TRANSACTION",
				"COMMIT", "ROLLBACK", "PREPARE", "TEMPORARY", "FOR UPDATE", "LOCK IN SHARE",
			} {
				if strings.Contains(sql, forbidden) {
					t.Fatalf("statement uses %q, which does not survive a pooler", forbidden)
				}
			}
			// UTC_TIMESTAMP rather than NOW: NOW is rendered in the session's
			// time zone, and two connections that disagree about it would be
			// writing timestamps from two different clocks into one column.
			if strings.Contains(sql, "NOW(") {
				t.Fatal("statement uses NOW(), whose value depends on the session time zone")
			}
			if !strings.Contains(sql, "UTC_TIMESTAMP(6)") {
				t.Fatal("statement does not take its time from the server")
			}
		})
	}
}

// Every read must return the server's current time alongside the lease, because
// the caller may only ever use the difference.
func TestTheLeaseProjectionReturnsServerTime(t *testing.T) {
	const projection = "holder, fence, acquired_at, renewed_at, expires_at, payload, UTC_TIMESTAMP(6)"
	if !strings.Contains(inspectSQL, projection) {
		t.Fatalf("the read does not return the standard projection %q:\n%s", projection, inspectSQL)
	}
}

// The guards are what stop a partitioned former leader from resurrecting its
// term. Losing any one of them loses the guarantee.
func TestRenewIsFullyGuarded(t *testing.T) {
	for _, guard := range []string{
		"name = ?",
		"holder = ?",
		"fence = ?",
		"expires_at > UTC_TIMESTAMP(6)",
	} {
		if !strings.Contains(renewSQL, guard) {
			t.Fatalf("renew statement is missing the guard %q", guard)
		}
	}
	// Release ends a term rather than continuing one, and it is guarded exactly
	// the same way: a stale fence must not be able to hand somebody else's
	// lease away.
	for _, guard := range []string{"name = ?", "holder = ?", "fence = ?", "expires_at > UTC_TIMESTAMP(6)"} {
		if !strings.Contains(releaseSQL, guard) {
			t.Fatalf("release statement is missing the guard %q", guard)
		}
	}
}

// The fence must advance on exactly one condition: reclaiming an expired lease.
// Extending a term that never lapsed keeps its token.
func TestFenceAdvancesOnlyOnReclaim(t *testing.T) {
	const rule = "fence       = CASE WHEN expires_at <= UTC_TIMESTAMP(6)\n" +
		"                       THEN fence + 1        ELSE fence       END"
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

// The order of the assignments in the upsert is what makes it correct, because
// MySQL lets a later one see what an earlier one wrote. Anything that reads the
// row as it was on disk has to come before holder is rewritten; anything that
// keys off the outcome has to come after. Reordering these is a silent change
// of meaning, so the order is pinned here as well as explained in sql.go.
func TestAcquireAssignmentOrderIsPinned(t *testing.T) {
	_, update, ok := strings.Cut(acquireSQL, "ON DUPLICATE KEY UPDATE")
	if !ok {
		t.Fatal("the acquire statement is not an upsert")
	}

	want := []string{"fence", "acquired_at", "holder", "renewed_at", "payload", "expires_at"}
	var got []string
	for _, line := range strings.Split(update, "\n") {
		column, _, isAssignment := strings.Cut(strings.TrimSpace(line), "= CASE")
		if isAssignment {
			got = append(got, strings.TrimSpace(column))
		}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("assignments run %v, want %v; see the comment on acquireSQL for what each position depends on", got, want)
	}

	// The three assignments after holder must test the holder rather than the
	// expiry: expires_at has not been written yet at that point, but reading it
	// there would be reading a value the statement is about to replace.
	tail := update[strings.Index(update, "renewed_at"):]
	if strings.Contains(tail, "expires_at <= UTC_TIMESTAMP(6)") {
		t.Fatalf("an assignment after holder still tests the old expiry:\n%s", tail)
	}
}

func statements() map[string]string {
	return map[string]string{
		"acquire": acquireSQL,
		"renew":   renewSQL,
		"release": releaseSQL,
		"inspect": inspectSQL,
	}
}
