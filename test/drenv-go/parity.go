// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

//go:build mage

package main

// Parity drift detection between the Python `drenv` and this Go port.
//
// The two projects live side by side for the foreseeable future, and fixes or
// enhancements from other teams often land only in the Python project. This
// check freezes a content hash of every upstream file drenv-go mirrors into
// parity.lock; when an upstream file changes (or a new addon appears), `make
// parity` fails and names exactly what drifted, so the lag is impossible to miss.
//
// Workflow:
//   - `make parity`        — fail if upstream drifted from the locked baseline.
//   - `make parity-update` — re-baseline parity.lock AFTER reconciling drenv-go
//     with the upstream change (the explicit "I reviewed this" acknowledgement).
//
// The tracked set is derived automatically so it stays honest with little toil:
//   - Every "<path>.py" the Go source cites in its "mirroring the Python ..."
//     comments (addons/*, providers/*, and core modules like kubectl.py).
//   - Every source file under each ported addon's directory (start.py plus its
//     start-data templates and kustomizations), so a changed template or a new
//     required $var is caught too.
//   - A small explicit supplement (parityExtraSources) for modules the Go
//     comments name in prose rather than by path.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const (
	// upstreamRoot is the Python drenv project, relative to this module root.
	// addonsSrcDir (magefile.go) is upstreamRoot + "/addons".
	upstreamRoot = "../drenv"
	// parityLockFile records the hash of every tracked upstream file at the last
	// reconciled sync. It is committed so drift is a reviewable diff.
	parityLockFile = "parity.lock"
)

// parityExtraSources are upstream files drenv-go mirrors that its comments refer
// to by module name rather than an explicit "<path>.py", so the Go-source scan
// cannot discover them. Add an entry here when a new such port lands.
var parityExtraSources = []string{
	"providers/minikube/__init__.py",
	"providers/minikube/dns.py",
	"providers/minikube/networkextension.py",
}

// parityAssetExts are the source extensions tracked when expanding a ported
// addon's directory (templates, kustomizations, helper scripts, logic).
var parityAssetExts = map[string]bool{
	".py": true, ".yaml": true, ".yml": true, ".json": true, ".sh": true,
}

// parityUnportedKnown are upstream addon directories that drenv-go deliberately
// does not port (see the "Not reimplemented" section of rtalur-readme.md). They
// are exempt from the "unported upstream addon" check so only genuinely new
// addons raise it. Add a directory here only with a documented reason.
var parityUnportedKnown = map[string]bool{
	"addons/cdi":      true,
	"addons/kubevirt": true,
	"addons/demo":     true,
	"addons/example":  true,
}

// pyRefRe matches a "<path>.py" reference anywhere in Go source.
var pyRefRe = regexp.MustCompile(`[A-Za-z0-9_][\w./-]*\.py`)

// Parity fails if any tracked upstream file drifted from parity.lock. It reports
// drifted, newly-tracked, and removed files, plus any unported upstream addon.
func Parity() error {
	tracked, err := parityTrackedFiles()
	if err != nil {
		return err
	}
	locked, err := readParityLock()
	if err != nil {
		return err
	}

	current := make(map[string]string, len(tracked))
	var drifted, added []string
	for _, rel := range tracked {
		sum, herr := hashFile(filepath.Join(upstreamRoot, filepath.FromSlash(rel)))
		if herr != nil {
			return herr
		}
		current[rel] = sum
		old, ok := locked[rel]
		switch {
		case !ok:
			added = append(added, rel)
		case old != sum:
			drifted = append(drifted, rel)
		}
	}

	var removed []string
	for rel := range locked {
		if _, ok := current[rel]; !ok {
			removed = append(removed, rel)
		}
	}

	unported, err := parityUnported(current)
	if err != nil {
		return err
	}

	sort.Strings(drifted)
	sort.Strings(added)
	sort.Strings(removed)
	sort.Strings(unported)

	report := func(title string, files []string) {
		if len(files) == 0 {
			return
		}
		fmt.Printf("\n%s\n", title)
		for _, f := range files {
			fmt.Printf("  %s\n", f)
		}
	}

	fmt.Printf("drenv-go parity check against %s (baseline: %s)\n", upstreamRoot, parityLockFile)
	fmt.Printf("tracked %d upstream file(s)\n", len(tracked))
	report("DRIFTED — upstream changed since last sync; review the drenv-go port:", drifted)
	report("NEW — tracked but missing from the lock; port then re-baseline:", added)
	report("REMOVED — in the lock but gone upstream; drenv-go may have dead code:", removed)
	report("UNPORTED — upstream addon with no drenv-go reference; port it or add to parityUnportedKnown:", unported)

	total := len(drifted) + len(added) + len(removed) + len(unported)
	if total == 0 {
		fmt.Println("\nin sync — no drift detected.")
		return nil
	}
	return fmt.Errorf("parity drift: %d item(s) need attention; after reconciling drenv-go run `make parity-update`", total)
}

// ParityUpdate rewrites parity.lock from the current tracked files. Run it only
// after reconciling drenv-go with the upstream changes — it is the explicit
// acknowledgement that the drift has been reviewed.
func ParityUpdate() error {
	tracked, err := parityTrackedFiles()
	if err != nil {
		return err
	}
	sums := make(map[string]string, len(tracked))
	for _, rel := range tracked {
		sum, herr := hashFile(filepath.Join(upstreamRoot, filepath.FromSlash(rel)))
		if herr != nil {
			return herr
		}
		sums[rel] = sum
	}
	if err := writeParityLock(sums); err != nil {
		return err
	}
	fmt.Printf("wrote %s with %d tracked file(s)\n", parityLockFile, len(sums))
	return nil
}

// parityTrackedFiles returns the sorted, de-duplicated set of upstream files
// drenv-go mirrors, expressed as slash paths relative to upstreamRoot.
func parityTrackedFiles() ([]string, error) {
	set := map[string]bool{}

	refs, err := parityRefsFromGo()
	if err != nil {
		return nil, err
	}
	for _, rel := range refs {
		set[rel] = true
	}
	for _, rel := range parityExtraSources {
		set[rel] = true
	}

	// Keep only files that exist, and expand each ported addon's directory so
	// templates and kustomizations under it are tracked too.
	for rel := range set {
		if !fileExists(filepath.Join(upstreamRoot, filepath.FromSlash(rel))) {
			// A cited path that no longer exists upstream (rename/removal). Drop
			// it here; the REMOVED report surfaces the lock entry instead.
			delete(set, rel)
			continue
		}
		if strings.HasPrefix(rel, "addons/") {
			assets, aerr := parityAddonAssets(path.Dir(rel))
			if aerr != nil {
				return nil, aerr
			}
			for _, a := range assets {
				set[a] = true
			}
		}
	}

	out := make([]string, 0, len(set))
	for rel := range set {
		out = append(out, rel)
	}
	sort.Strings(out)
	return out, nil
}

// parityRefsFromGo scans the Go source for "<path>.py" references, normalizes
// them relative to upstreamRoot, and returns those that resolve to a real file.
func parityRefsFromGo() ([]string, error) {
	seen := map[string]bool{}
	for _, root := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(p, ".go") {
				return nil
			}
			data, rerr := os.ReadFile(p)
			if rerr != nil {
				return rerr
			}
			for _, m := range pyRefRe.FindAllString(string(data), -1) {
				rel := strings.TrimPrefix(m, "test/drenv/")
				if fileExists(filepath.Join(upstreamRoot, filepath.FromSlash(rel))) {
					seen[rel] = true
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	out := make([]string, 0, len(seen))
	for rel := range seen {
		out = append(out, rel)
	}
	return out, nil
}

// parityAddonAssets returns the tracked source files under an addon directory
// (slash paths relative to upstreamRoot), skipping tests and caches.
func parityAddonAssets(addonDir string) ([]string, error) {
	base := filepath.Join(upstreamRoot, filepath.FromSlash(addonDir))
	var out []string
	err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "__pycache__" {
				return fs.SkipDir
			}
			return nil
		}
		name := d.Name()
		// cache.py is the per-addon kustomize-cache hook, a feature drenv-go
		// deliberately drops (see rtalur-readme.md); it is identical boilerplate
		// across addons and would only add noise.
		if name == "cache.py" || strings.HasSuffix(name, "_test.py") || !parityAssetExts[filepath.Ext(name)] {
			return nil
		}
		rel, rerr := filepath.Rel(upstreamRoot, p)
		if rerr != nil {
			return rerr
		}
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	return out, err
}

// parityUnported returns upstream addons (directories with a start.py) that no
// tracked file references and that are not in parityUnportedKnown.
func parityUnported(tracked map[string]string) ([]string, error) {
	covered := map[string]bool{}
	for rel := range tracked {
		if strings.HasPrefix(rel, "addons/") {
			covered[path.Dir(rel)] = true
		}
	}

	var out []string
	addonsBase := filepath.Join(upstreamRoot, "addons")
	err := filepath.WalkDir(addonsBase, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() != "start.py" {
			return nil
		}
		rel, rerr := filepath.Rel(upstreamRoot, filepath.Dir(p))
		if rerr != nil {
			return rerr
		}
		dir := filepath.ToSlash(rel)
		if covered[dir] || parityUnportedKnown[dir] {
			return nil
		}
		out = append(out, dir)
		return nil
	})
	return out, err
}

// readParityLock parses parity.lock into a rel-path→hash map. A missing lock is
// an error pointing at parity-update, since drift cannot be judged without it.
func readParityLock() (map[string]string, error) {
	data, err := os.ReadFile(parityLockFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%s not found — run `make parity-update` to create the baseline", parityLockFile)
		}
		return nil, err
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("%s: malformed line %q", parityLockFile, line)
		}
		out[fields[1]] = fields[0]
	}
	return out, nil
}

// writeParityLock writes the hash map to parity.lock, sorted by path, with a
// header explaining the file (skipped by readParityLock).
func writeParityLock(sums map[string]string) error {
	rels := make([]string, 0, len(sums))
	for rel := range sums {
		rels = append(rels, rel)
	}
	sort.Strings(rels)

	var b strings.Builder
	b.WriteString("# SPDX-FileCopyrightText: The RamenDR authors\n")
	b.WriteString("# SPDX-License-Identifier: Apache-2.0\n")
	b.WriteString("#\n")
	b.WriteString("# drenv-go parity baseline: sha256 of every upstream (Python drenv) file\n")
	b.WriteString("# this Go port mirrors. Generated by `make parity-update`; checked by\n")
	b.WriteString("# `make parity`. Re-baseline ONLY after reconciling drenv-go with the\n")
	b.WriteString("# upstream change. Format: <sha256>  <path relative to ../drenv>\n")
	for _, rel := range rels {
		fmt.Fprintf(&b, "%s  %s\n", sums[rel], rel)
	}
	return os.WriteFile(parityLockFile, []byte(b.String()), 0o644)
}

func hashFile(p string) (string, error) {
	data, err := os.ReadFile(p)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func fileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}
