# Targets are grouped by what they need: `test` and `lint` need nothing but Go,
# everything below `test-int` needs Docker.

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := check

COVERDIR := .cover
MODULES := . ./postgres

# Two paths to the same database. The suites run against both, because "works
# directly but not through the pooler" is this library's likeliest bug.
export LEADERELECTION_POSTGRES_DSN ?= postgres://leaderelection:leaderelection@127.0.0.1:55432/leaderelection?sslmode=disable
export LEADERELECTION_PGBOUNCER_DSN ?= postgres://leaderelection:leaderelection@127.0.0.1:56432/leaderelection?sslmode=disable

# Production packages only. Test-support packages are excluded from the
# coverage floor because their uncovered statements are assertion failures --
# reachable only by making a test fail. This is the project's only exclusion,
# and it is per package, never per line.
COVER_EXCLUDE := /storetest,/internal/assert,/tools/

.PHONY: check
check: lint test ## Everything that runs without Docker.

.PHONY: test
test: ## Unit tests, race detector on, no external services.
	@for m in $(MODULES); do \
		echo "==> unit $$m"; \
		(cd $$m && go test -race -timeout 5m ./...); \
	done

.PHONY: test-int
test-int: up ## Integration tests against real PostgreSQL, direct and via PgBouncer.
	go test -tags=integration -race -timeout 10m ./postgres/...

.PHONY: test-chaos
test-chaos: up ## Chaos scenarios: partitions, hangs, restarts, pooler failures.
	cd tests && go test -tags=chaos -race -timeout 30m ./...

.PHONY: test-soak
test-soak: up ## Long scenarios: randomised churn, rolling restarts, pool exhaustion.
	cd tests && go test -tags=soak -race -timeout 60m ./...

.PHONY: test-all
test-all: test test-int test-chaos ## Every suite except the soak.

# Seed corpora and any committed regression inputs run as ordinary tests, so
# `make test` already exercises every target. This is for going looking.
FUZZTIME ?= 30s

.PHONY: fuzz
fuzz: ## Explore each fuzz target for FUZZTIME (default 30s, needs no Docker).
	@for m in $(MODULES); do \
		(cd $$m && for pkg in $$(go list ./...); do \
			for target in $$(go test -list '^Fuzz' $$pkg | grep '^Fuzz' || true); do \
				echo "==> $$target in $$pkg for $(FUZZTIME)"; \
				go test -run '^$$' -fuzz "^$$target$$" -fuzztime $(FUZZTIME) $$pkg || exit 1; \
			done; \
		done) || exit 1; \
	done

.PHONY: cover
cover: up ## Merge unit and integration coverage and enforce the floor.
	@rm -rf $(COVERDIR) && mkdir -p $(COVERDIR)
	go test -covermode=atomic -coverpkg=./... -coverprofile=$(COVERDIR)/core.out ./...
	cd postgres && go test -tags=integration -covermode=atomic \
		-coverpkg=github.com/brualan/leaderelection/postgres \
		-coverprofile=../$(COVERDIR)/postgres.out ./...
	go run ./tools/covercheck -min 100 -exclude '$(COVER_EXCLUDE)' $(COVERDIR)/*.out

.PHONY: cover-html
cover-html: cover ## Open the merged profile in a browser.
	go tool cover -html=$(COVERDIR)/core.out

.PHONY: lint
lint: ## Vet, formatting and golangci-lint if it is installed.
	@for m in $(MODULES) ./tools ./tests; do \
		[ -f $$m/go.mod ] || continue; \
		echo "==> vet $$m"; \
		(cd $$m && go vet ./...); \
	done
	@unformatted=$$(gofmt -l . | grep -v '^\.cover/' || true); \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi
	@if command -v golangci-lint >/dev/null; then \
		for m in $(MODULES); do echo "==> golangci-lint $$m"; (cd $$m && golangci-lint run); done; \
	else \
		echo "golangci-lint not installed, skipping"; \
	fi

.PHONY: up
up: ## Start PostgreSQL and PgBouncer and wait for both to be healthy.
	docker compose up -d --wait

.PHONY: down
down: ## Stop the test infrastructure and delete its data.
	docker compose down -v --remove-orphans

.PHONY: psql
psql: ## Open a shell on the test database.
	docker compose exec postgres psql -U leaderelection -d leaderelection

.PHONY: tidy
tidy: ## Tidy the modules that can resolve outside the workspace.
	@# The submodules require the core module by version, and go mod tidy
	@# ignores go.work, so it cannot resolve that requirement until the core
	@# module has a tag. Their go.mod files are maintained by hand until then;
	@# see CONTRIBUTING.md.
	@for m in . ./tools; do \
		echo "==> tidy $$m"; \
		(cd $$m && GOWORK=off go mod tidy); \
	done

.PHONY: help
help: ## List targets.
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-14s\033[0m %s\n", $$1, $$2}'
