-- Runs once, when the MySQL container initialises its data directory.
--
-- ProxySQL's monitor module connects on its own to decide whether a backend is
-- alive. Without an account it can use, it shuns the only backend there is and
-- every scenario fails for a reason nowhere near the one it is testing.
--
-- The authentication plugin is deliberately left at the server's default, which
-- differs between the versions in the CI matrix. Pinning it would have meant
-- one flag for MySQL 8.0 and another for 8.4, and would have quietly stopped
-- testing whether the pooler can authenticate the way a real deployment does.
CREATE USER IF NOT EXISTS 'monitor'@'%' IDENTIFIED BY 'monitor';
GRANT USAGE, REPLICATION CLIENT ON *.* TO 'monitor'@'%';
FLUSH PRIVILEGES;
