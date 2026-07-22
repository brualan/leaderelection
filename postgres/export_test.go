package postgres

import "github.com/jackc/pgx/v5/pgxpool"

// ApplyPoolerSafeDefaultsForTest exposes the pool configuration [Connect]
// applies, so that a test can assert on it without opening a connection.
func ApplyPoolerSafeDefaultsForTest(cfg *pgxpool.Config) { applyPoolerSafeDefaults(cfg) }

// NewWithQuerierForTest builds a Store over an arbitrary querier, which is how
// the driver's failure paths are reached without provoking a real database
// into failing in exactly the right way.
func NewWithQuerierForTest(db querier, opts ...Option) (*Store, error) {
	return newStore(db, func() {}, opts...)
}
