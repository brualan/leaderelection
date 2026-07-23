package harness

import (
	"context"

	"github.com/brualan/leaderelection"
	"github.com/brualan/leaderelection/mysql"
	"github.com/brualan/leaderelection/postgres"
)

// The scenarios are written against the election, not against a dialect. Both
// stores make the same promises, so both have to survive the same partitions,
// the same hangs and the same restarts -- and the scenario that finds a
// difference is worth more than any amount of agreement about the SQL.
//
// A Backend is everything a scenario needs in order to be run twice.

// Store is what a participant talks to: a [leaderelection.Store] that also
// knows how to create its own table and how to let go of its connections.
type Store interface {
	leaderelection.Store

	EnsureSchema(ctx context.Context) error
	Close()
}

// Backend describes one database the scenarios can run against.
type Backend struct {
	// Name labels the subtest.
	Name string

	// Service and Pooler are the compose services holding the database and the
	// connection pooler in front of it. They are what a scenario stops, kills
	// or pauses.
	Service string
	Pooler  string

	// Direct and Pooled are the two upstreams a proxy can forward to, as seen
	// from inside the toxiproxy container.
	Direct string
	Pooled string

	// Admin is the published address of the database itself, used to create the
	// schema before any proxy exists -- so that a scenario which breaks every
	// proxy still starts from a working table.
	Admin string

	// DSNTemplate is the connection string with %s standing for host:port.
	DSNTemplate string

	// Open builds a store over dsn.
	Open func(ctx context.Context, dsn string) (Store, error)
}

// Upstreams returns the two routes to this backend, in the order a scenario
// should run them.
func (b *Backend) Upstreams() []struct{ Name, Addr string } {
	return []struct{ Name, Addr string }{
		{b.Name + "/direct", b.Direct},
		{b.Name + "/" + b.Pooler, b.Pooled},
	}
}

// PostgresBackend is PostgreSQL reached directly and through PgBouncer in
// transaction pooling mode.
var PostgresBackend = &Backend{
	Name:        "postgres",
	Service:     "postgres",
	Pooler:      "pgbouncer",
	Direct:      "postgres:5432",
	Pooled:      "pgbouncer:5432",
	Admin:       "127.0.0.1:55432",
	DSNTemplate: "postgres://leaderelection:leaderelection@%s/leaderelection?sslmode=disable",
	Open: func(ctx context.Context, dsn string) (Store, error) {
		return postgres.Connect(ctx, dsn)
	},
}

// MySQLBackend is MySQL reached directly and through ProxySQL.
var MySQLBackend = &Backend{
	Name:        "mysql",
	Service:     "mysql",
	Pooler:      "proxysql",
	Direct:      "mysql:3306",
	Pooled:      "proxysql:6033",
	Admin:       "127.0.0.1:53306",
	DSNTemplate: "leaderelection:leaderelection@tcp(%s)/leaderelection",
	Open: func(ctx context.Context, dsn string) (Store, error) {
		return mysql.Connect(ctx, dsn)
	},
}

// Backends is every database the chaos suite runs against.
var Backends = []*Backend{PostgresBackend, MySQLBackend}
