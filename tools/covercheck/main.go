// Command covercheck merges Go coverage profiles and enforces a per-package
// floor.
//
// It exists because this project claims full coverage of its production
// packages, and a claim nobody checks decays within a release or two. Merging
// is part of the job: unit tests and integration tests cover different halves
// of the driver, and only their union is meaningful.
//
// Test-support packages are excluded by name rather than by line. Their
// uncovered statements are the t.Fatalf calls inside assertions, reachable
// only when a test fails, so requiring coverage there would mean writing tests
// that fail on purpose. That is the only exclusion this project allows, and it
// is a decision about which packages are production code, not a licence to
// ignore individual lines.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

func main() {
	var (
		exclude = flag.String("exclude", "", "comma-separated package path substrings to leave out")
		min     = flag.Float64("min", 100, "required coverage percentage per package")
		verbose = flag.Bool("v", false, "print every package, not just the failures")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: covercheck [flags] profile...\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	profiles, err := merge(flag.Args())
	if err != nil {
		fmt.Fprintf(os.Stderr, "covercheck: %v\n", err)
		os.Exit(1)
	}

	var excluded []string
	for _, e := range strings.Split(*exclude, ",") {
		if e = strings.TrimSpace(e); e != "" {
			excluded = append(excluded, e)
		}
	}

	if !report(profiles, excluded, *min, *verbose) {
		os.Exit(1)
	}
}

// block is one coverage region: a span of source with a statement count and
// the number of times it was executed.
type block struct {
	span  string
	stmts int
	count int
}

// merge reads every profile and sums the execution counts of identical blocks.
func merge(paths []string) (map[string][]block, error) {
	merged := map[string]map[string]block{}

	for _, p := range paths {
		// Coverage profile paths are explicitly supplied by the CLI caller.
		f, err := os.Open(p) //nolint:gosec // Profile paths are explicitly supplied by the repository tooling caller.
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" || strings.HasPrefix(line, "mode:") {
				continue
			}
			file, b, err := parse(line)
			if err != nil {
				_ = f.Close() //nolint:errcheck // Preserve the profile parsing error.
				return nil, fmt.Errorf("%s: %w", p, err)
			}
			if merged[file] == nil {
				merged[file] = map[string]block{}
			}
			existing, ok := merged[file][b.span]
			if ok {
				b.count += existing.count
			}
			merged[file][b.span] = b
		}
		err = scanner.Err()
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
	}

	out := map[string][]block{}
	for file, blocks := range merged {
		for _, b := range blocks {
			out[file] = append(out[file], b)
		}
	}
	return out, nil
}

// parse reads a line of the form
//
//	path/file.go:1.2,3.4 5 6
func parse(line string) (string, block, error) {
	colon := strings.LastIndex(line, ":")
	if colon < 0 {
		return "", block{}, fmt.Errorf("malformed profile line %q", line)
	}
	file, rest := line[:colon], line[colon+1:]

	fields := strings.Fields(rest)
	if len(fields) != 3 {
		return "", block{}, fmt.Errorf("malformed profile line %q", line)
	}
	stmts, err := strconv.Atoi(fields[1])
	if err != nil {
		return "", block{}, fmt.Errorf("malformed statement count in %q", line)
	}
	count, err := strconv.Atoi(fields[2])
	if err != nil {
		return "", block{}, fmt.Errorf("malformed execution count in %q", line)
	}
	return file, block{span: fields[0], stmts: stmts, count: count}, nil
}

type pkgCoverage struct {
	name      string
	covered   int
	total     int
	uncovered []string
}

func (p pkgCoverage) percent() float64 {
	if p.total == 0 {
		return 100
	}
	return 100 * float64(p.covered) / float64(p.total)
}

// report prints per-package coverage and returns whether every included
// package met the floor.
func report(profiles map[string][]block, excluded []string, min float64, verbose bool) bool {
	packages := map[string]*pkgCoverage{}

	for file, blocks := range profiles {
		pkg := path.Dir(file)
		if skip(pkg, excluded) {
			continue
		}
		p := packages[pkg]
		if p == nil {
			p = &pkgCoverage{name: pkg}
			packages[pkg] = p
		}
		for _, b := range blocks {
			p.total += b.stmts
			if b.count > 0 {
				p.covered += b.stmts
				continue
			}
			p.uncovered = append(p.uncovered, file+":"+b.span)
		}
	}

	names := make([]string, 0, len(packages))
	for name := range packages {
		names = append(names, name)
	}
	sort.Strings(names)

	ok := true
	var covered, total int
	for _, name := range names {
		p := packages[name]
		covered += p.covered
		total += p.total

		pass := p.percent() >= min
		if !pass {
			ok = false
		}
		if verbose || !pass {
			fmt.Printf("%-6.1f%% %s\n", p.percent(), name)
		}
		if !pass {
			sort.Strings(p.uncovered)
			for _, u := range p.uncovered {
				fmt.Printf("         uncovered: %s\n", u)
			}
		}
	}

	overall := 100.0
	if total > 0 {
		overall = 100 * float64(covered) / float64(total)
	}
	if ok {
		fmt.Printf("covercheck: %.1f%% across %d packages, floor %.1f%%\n", overall, len(names), min)
	} else {
		fmt.Fprintf(os.Stderr, "covercheck: FAILED, %.1f%% across %d packages, floor %.1f%%\n",
			overall, len(names), min)
	}
	return ok
}

func skip(pkg string, excluded []string) bool {
	for _, e := range excluded {
		if strings.Contains(pkg, e) {
			return true
		}
	}
	return false
}
