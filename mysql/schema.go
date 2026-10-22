package mysql

import (
	"context"
	"fmt"
)

// SchemaSQL returns the initial table DDL without connecting to a database.
// It accepts the same options and defaults as [New] and [Connect]. Apply the
// SQL through your migration tool with a separate privileged identity before
// starting services. It does not upgrade or validate an existing table.
func SchemaSQL(opts ...Option) (string, error) {
	o := options{table: DefaultTable}
	for _, opt := range opts {
		opt(&o)
	}
	return Schema(o.database, o.table)
}

// CheckSchema checks that the configured table and required columns can be
// selected. It performs one read-only query, returns no lease data, and works
// on an empty table. It never creates, alters, or repairs the schema.
//
// This is a connectivity and column-access check, not a migration validator:
// it does not verify column types, constraints, indexes, or INSERT/UPDATE
// privileges. Schema correctness remains the migration process's responsibility.
func (s *Store) CheckSchema(ctx context.Context) error {
	// s.table contains only identifiers validated and quoted by qualify at construction.
	query := "SELECT name, holder, fence, acquired_at, renewed_at, expires_at, payload FROM " + s.table + " WHERE 1 = 0" //nolint:gosec
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("leaderelection/mysql: check schema: %w", wrap(err))
	}
	// Iteration below checks rows.Err, including automatic close errors at EOF.
	defer func() { _ = rows.Close() }()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("leaderelection/mysql: check schema: %w", wrap(err))
	}
	return nil
}
