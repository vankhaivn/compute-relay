package packaging

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/safefs"
)

// Project keeps the configured root handle open. Callers must not Close while a Plan or
// Snapshot is in use. Protected relative paths are operator-defined state/cache roots,
// excluded in addition to defaults and .computeignore. They cannot be re-included.
type Project struct {
	root      *safefs.Root
	limits    Limits
	protected []string
}

func OpenProject(path string, limits Limits, protected []string) (*Project, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	for _, p := range protected {
		if !safefs.ValidPath(p) {
			return nil, ErrPath
		}
	}
	root, err := safefs.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	return &Project{root: root, limits: limits, protected: append([]string(nil), protected...)}, nil
}
func (p *Project) Close() error { return p.root.Close() }
func (p *Project) excluded(name string, rules ignoreRules) bool {
	if rules.excludes(name) {
		return true
	}
	for _, prefix := range p.protected {
		if prefix == "." || strings.EqualFold(name, prefix) || strings.HasPrefix(strings.ToLower(name), strings.ToLower(prefix)+"/") {
			return true
		}
	}
	return false
}

type savedFile struct {
	File
	stamp os.FileInfo
}
type directory struct {
	name  string
	stamp os.FileInfo
}
type Plan struct {
	project *Project
	files   []savedFile
	dirs    []directory
	ignore  []byte
	preview Preview
}

func (p *Plan) Preview() Preview {
	result := p.preview
	result.Manifest.Files = append([]File(nil), result.Manifest.Files...)
	return result
}
func (p *Project) loadIgnore(ctx context.Context) (ignoreRules, []byte, error) {
	f, err := p.root.Open(".computeignore")
	if errors.Is(err, safefs.ErrNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil || !before.Mode().IsRegular() {
		return nil, nil, ErrPath
	}
	r := &boundReader{r: &contextReader{ctx: ctx, r: f}, left: 64 << 10}
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, err
	}
	after, err := f.Stat()
	if err != nil || !safefs.Unchanged(before, after) {
		return nil, nil, ErrChanged
	}
	rules, err := parseIgnore(string(b))
	return rules, b, err
}

// Prepare takes an explicit file/directory selection. An empty selection or "." is not
// interpreted as permission to archive a home directory or repository automatically.
func (p *Project) Prepare(ctx context.Context, includes []string) (*Plan, error) {
	if len(includes) == 0 || len(includes) > p.limits.MaxFiles {
		return nil, ErrInvalid
	}
	rules, ignore, err := p.loadIgnore(ctx)
	if err != nil {
		return nil, err
	}
	plan := &Plan{project: p, ignore: ignore}
	paths := newPaths()
	visited := map[string]bool{}
	entries := 0
	var walk func(string, int) error
	walk = func(name string, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries++
		if entries > p.limits.MaxEntries || depth > p.limits.MaxDepth {
			return ErrLimit
		}
		if name == "." || len(name) > 240 || !safefs.ValidPath(name) {
			return ErrPath
		}
		if p.excluded(name, rules) {
			plan.preview.ExcludedEntries++
			return nil
		}
		if visited[name] {
			return ErrInvalid
		}
		visited[name] = true
		f, err := p.root.Open(name)
		if err != nil {
			return err
		}
		defer f.Close()
		stamp, err := f.Stat()
		if err != nil {
			return ErrUnavailable
		}
		if stamp.IsDir() {
			plan.dirs = append(plan.dirs, directory{name, stamp})
			for {
				children, err := f.ReadDir(128)
				if err != nil && err != io.EOF {
					return ErrUnavailable
				}
				for _, child := range children {
					if e := walk(name+"/"+child.Name(), depth+1); e != nil {
						return e
					}
				}
				if err == io.EOF {
					break
				}
			}
			after, err := f.Stat()
			if err != nil || !safefs.Unchanged(stamp, after) {
				return ErrChanged
			}
			return nil
		}
		if len(plan.files) >= p.limits.MaxFiles || stamp.Size() < 0 || stamp.Size() > p.limits.MaxExpandedBytes-plan.preview.ExpandedBytes {
			return ErrLimit
		}
		if err := paths.add(name); err != nil {
			return err
		}
		entry := savedFile{File: File{Path: name, Bytes: stamp.Size(), Executable: stamp.Mode()&0o111 != 0}, stamp: stamp}
		// Fail preview before writing anything if this path cannot be encoded as USTAR.
		if err := tar.NewWriter(io.Discard).WriteHeader(header("code/"+name, entry.Bytes, entry.Executable)); err != nil {
			return ErrPath
		}
		sha, err := copyFile(ctx, p.root, entry, io.Discard)
		if err != nil {
			return err
		}
		entry.SHA256 = sha
		plan.preview.ExpandedBytes += entry.Bytes
		plan.files = append(plan.files, entry)
		return nil
	}
	selection := append([]string(nil), includes...)
	sort.Strings(selection)
	for _, name := range selection {
		if err := walk(name, len(strings.Split(name, "/"))); err != nil {
			return nil, err
		}
	}
	if len(plan.files) == 0 {
		return nil, ErrInvalid
	}
	sort.Slice(plan.files, func(i, j int) bool { return plan.files[i].Path < plan.files[j].Path })
	plan.preview.Manifest = Manifest{Version: Version, Files: make([]File, len(plan.files))}
	for i, f := range plan.files {
		plan.preview.Manifest.Files[i] = f.File
	}
	encoded, _ := manifestBytes(plan.preview.Manifest)
	if int64(len(encoded)) > p.limits.MaxManifestBytes {
		return nil, ErrLimit
	}
	plan.preview.ManifestSHA256 = digest(encoded)
	plan.preview.SecretScan = "heuristic-passed"
	if err := plan.checkTree(ctx); err != nil {
		return nil, err
	}
	return plan, nil
}
func (p *Plan) checkTree(ctx context.Context) error {
	for _, d := range p.dirs {
		if err := ctx.Err(); err != nil {
			return err
		}
		f, err := p.project.root.Open(d.name)
		if err != nil {
			return ErrChanged
		}
		info, err := f.Stat()
		_ = f.Close()
		if err != nil || !safefs.Unchanged(d.stamp, info) {
			return ErrChanged
		}
	}
	_, current, err := p.project.loadIgnore(ctx)
	if err != nil {
		return err
	}
	if string(current) != string(p.ignore) {
		return ErrChanged
	}
	return nil
}

// Snapshot is a raw, single-file input (not a tar archive). Its first pass computes the
// preview digest; Write reopens through the held root and compares both identity and bytes.
type Snapshot struct {
	project *Project
	file    savedFile
}

func (p *Project) SnapshotFile(ctx context.Context, name string, maxBytes int64) (*Snapshot, error) {
	if maxBytes < 0 || maxBytes > 4<<30 || name == "." || !safefs.ValidPath(name) {
		return nil, ErrPath
	}
	rules, _, err := p.loadIgnore(ctx)
	if err != nil {
		return nil, err
	}
	if p.excluded(name, rules) {
		return nil, ErrPath
	}
	f, err := p.root.Open(name)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	_ = f.Close()
	if err != nil || !info.Mode().IsRegular() {
		return nil, ErrPath
	}
	if info.Size() < 0 || info.Size() > maxBytes {
		return nil, ErrLimit
	}
	entry := savedFile{File: File{Path: name, Bytes: info.Size(), Executable: info.Mode()&0o111 != 0}, stamp: info}
	hash, err := copyFile(ctx, p.root, entry, io.Discard)
	if err != nil {
		return nil, err
	}
	entry.SHA256 = hash
	return &Snapshot{project: p, file: entry}, nil
}
func (s *Snapshot) Metadata() File { return s.file.File }
func (s *Snapshot) Write(ctx context.Context, w io.Writer) error {
	_, err := copyFile(ctx, s.project.root, s.file, w)
	return err
}

func copyFile(ctx context.Context, root *safefs.Root, entry savedFile, w io.Writer) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f, err := root.Open(entry.Path)
	if err != nil {
		return "", ErrChanged
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !safefs.Unchanged(entry.stamp, info) {
		return "", ErrChanged
	}
	hash := sha256.New()
	scanner := secretScanner{}
	buf := make([]byte, BufferBytes)
	remaining := entry.Bytes
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := f.Read(buf[:min(int64(len(buf)), remaining)])
		if n > 0 {
			if e := scanner.check(buf[:n]); e != nil {
				return "", e
			}
			written, e := w.Write(buf[:n])
			if e != nil {
				return "", e
			}
			if written != n {
				return "", io.ErrShortWrite
			}
			_, _ = hash.Write(buf[:n])
			remaining -= int64(n)
		}
		if err != nil {
			if err == io.EOF && remaining == 0 {
				break
			}
			return "", ErrChanged
		}
		if n == 0 {
			return "", ErrChanged
		}
	}
	var probe [1]byte
	n, err := f.Read(probe[:])
	if n != 0 || err != io.EOF {
		return "", ErrChanged
	}
	after, err := f.Stat()
	if err != nil || !safefs.Unchanged(entry.stamp, after) {
		return "", ErrChanged
	}
	// Detect replacement/unlink under the same path after the descriptor was opened.
	current, err := root.Open(entry.Path)
	if err != nil {
		return "", ErrChanged
	}
	currentInfo, err := current.Stat()
	_ = current.Close()
	if err != nil || !safefs.Unchanged(entry.stamp, currentInfo) {
		return "", ErrChanged
	}
	sum := hex.EncodeToString(hash.Sum(nil))
	if entry.SHA256 != "" && sum != entry.SHA256 {
		return "", ErrChanged
	}
	return sum, nil
}

type contextReader struct {
	ctx   context.Context
	r     io.Reader
	empty int
}

func (r *contextReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.r.Read(b)
	if n == 0 && err == nil {
		r.empty++
		if r.empty >= 100 {
			return 0, io.ErrNoProgress
		}
	} else {
		r.empty = 0
	}
	return n, err
}
