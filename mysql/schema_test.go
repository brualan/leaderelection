package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSchemaSQL(t *testing.T) {
	for _, tc := range []struct{ namespace, table string }{{"", DefaultTable}, {"coord", "leases"}} {
		want, err := Schema(tc.namespace, tc.table)
		if err != nil {
			t.Fatal(err)
		}
		got, err := SchemaSQL(WithTable(tc.namespace, tc.table))
		if err != nil || got != want {
			t.Fatalf("got %q, %v; want %q", got, err, want)
		}
	}
	want, _ := Schema("", DefaultTable)
	if got, err := SchemaSQL(); err != nil || got != want {
		t.Fatalf("default: %q, %v", got, err)
	}
	if _, err := SchemaSQL(WithTable("", "invalid;table")); err == nil {
		t.Fatal("accepted invalid identifier")
	}
}

func TestCheckSchema(t *testing.T) {
	for _, tc := range []struct {
		name                string
		queryErr, streamErr error
	}{
		{name: "empty table"},
		{name: "query failure", queryErr: errBoom},
		{name: "stream failure", streamErr: errBoom},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeDB{queryErr: tc.queryErr, rowErr: tc.streamErr}
			store := newTestStore(t, f, WithTable("coord", "leases"))
			err := store.CheckSchema(context.Background())
			wantErr := tc.queryErr != nil || tc.streamErr != nil
			if wantErr && !errors.Is(err, errBoom) {
				t.Fatalf("error not preserved: %v", err)
			}
			if !wantErr && err != nil {
				t.Fatal(err)
			}
			query := f.lastQuerySQL
			if !strings.HasPrefix(query, "SELECT name, holder, fence, acquired_at, renewed_at, expires_at, payload FROM "+store.Table()) || !strings.HasSuffix(query, " WHERE 1 = 0") {
				t.Fatalf("unexpected query: %s", query)
			}
			if f.execs != 0 {
				t.Fatal("schema check performed a write")
			}
		})
	}
}
