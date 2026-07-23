package mysql

import driver "github.com/go-sql-driver/mysql"

// ApplyPoolerSafeDefaultsForTest exposes the connection configuration [Connect]
// applies, so that a test can assert on it without opening a connection.
func ApplyPoolerSafeDefaultsForTest(cfg *driver.Config) { applyPoolerSafeDefaults(cfg) }
