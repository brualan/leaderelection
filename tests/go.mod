// Chaos and end-to-end tests. A separate module so that its infrastructure
// dependencies never reach anyone importing the library.
module github.com/brualan/leaderelection/tests

go 1.23.0

require (
	github.com/brualan/leaderelection v0.6.0
	github.com/brualan/leaderelection/mysql v0.1.0
	github.com/brualan/leaderelection/postgres v0.1.0
	github.com/go-sql-driver/mysql v1.9.3
	github.com/jackc/pgx/v5 v5.7.6
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/sync v0.13.0 // indirect
	golang.org/x/text v0.24.0 // indirect
)
