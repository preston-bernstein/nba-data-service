// Package opsec guards against leaking real home-network addresses into this
// public repo. It contains no production code, only a regression test.
package opsec

import (
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// rfc1918Pattern matches any literal RFC1918 private IPv4 address
// (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16). It is intentionally a
// generic range pattern, not a list of specific addresses, so this test
// never itself embeds a sensitive literal.
var rfc1918Pattern = regexp.MustCompile(
	`\b(?:10(?:\.\d{1,3}){3}|172\.(?:1[6-9]|2\d|3[0-1])(?:\.\d{1,3}){2}|192\.168(?:\.\d{1,3}){2})\b`,
)

// knownSafeFixture is the one RFC1918 literal this repo permits: it is not
// reachable on the real home network and exists so a test can legitimately
// exercise private-IP-rejection logic without risking a real host address.
const knownSafeFixture = "172.16.0.1"

const selfPath = "internal/opsec/no_private_literals_test.go"

// TestNoPrivateIPLiteralsInTrackedSource fails if a real RFC1918 address
// literal reappears anywhere in the tracked working tree. Scope is
// deliberately "git ls-files" (tracked files only) so a developer's local
// .env or other gitignored file never trips it.
func TestNoPrivateIPLiteralsInTrackedSource(t *testing.T) {
	root, listing := trackedFiles(t)

	for _, rel := range listing {
		if rel == selfPath {
			continue
		}

		data, err := os.ReadFile(root + "/" + rel)
		if err != nil {
			// Not every tracked path is a readable regular file (e.g. a
			// submodule gitlink); skip anything we can't read as text.
			continue
		}

		for lineNo, line := range strings.Split(string(data), "\n") {
			for _, match := range rfc1918Pattern.FindAllString(line, -1) {
				if match == knownSafeFixture {
					continue
				}
				t.Errorf("%s:%d: literal RFC1918 address %q found in tracked source; "+
					"use an env var with a loopback default for functional code, a "+
					"placeholder for docs, or %s for a test fixture that genuinely "+
					"needs a private address", rel, lineNo+1, match, knownSafeFixture)
			}
		}
	}
}

// TestNoForbiddenHostAliasesInTrackedSource optionally checks for specific
// real host aliases (e.g. SSH config names), sourced entirely from an env
// var so this test never hardcodes a sensitive string and stays green in
// any fork or CI run that doesn't set it.
func TestNoForbiddenHostAliasesInTrackedSource(t *testing.T) {
	raw := os.Getenv("OPSEC_FORBIDDEN_LITERALS")
	if raw == "" {
		t.Skip("OPSEC_FORBIDDEN_LITERALS not set, skipping host-alias check")
	}

	var forbidden []string
	for _, s := range strings.Split(raw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			forbidden = append(forbidden, s)
		}
	}
	if len(forbidden) == 0 {
		t.Skip("OPSEC_FORBIDDEN_LITERALS set but empty, skipping host-alias check")
	}

	root, listing := trackedFiles(t)

	for _, rel := range listing {
		if rel == selfPath {
			continue
		}
		data, err := os.ReadFile(root + "/" + rel)
		if err != nil {
			continue
		}
		for lineNo, line := range strings.Split(string(data), "\n") {
			for _, alias := range forbidden {
				if strings.Contains(line, alias) {
					t.Errorf("%s:%d: forbidden literal %q found in tracked source", rel, lineNo+1, alias)
				}
			}
		}
	}
}

// trackedFiles returns the repo root and the list of git-tracked file paths
// relative to it, skipping the test via t.Skip if git is unavailable.
func trackedFiles(t *testing.T) (string, []string) {
	t.Helper()

	root, err := gitOutput("rev-parse", "--show-toplevel")
	if err != nil {
		t.Skipf("not inside a git checkout, skipping opsec literal guard: %v", err)
	}

	listing, err := gitOutput("-C", root, "ls-files")
	if err != nil {
		t.Skipf("could not list tracked files, skipping opsec literal guard: %v", err)
	}

	var files []string
	for _, rel := range strings.Split(listing, "\n") {
		if rel != "" {
			files = append(files, rel)
		}
	}
	return root, files
}

func gitOutput(args ...string) (string, error) {
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(out), "\n"), nil
}
