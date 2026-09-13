package contracts

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func validateLocalRefs(apiRoot string) error {
	return filepath.WalkDir(apiRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".json" || entry.Name() == filepath.Base(lockPath) {
			return nil
		}
		value, err := readJSONValue(path)
		if err != nil {
			return fmt.Errorf("parse %s: %w", filepath.ToSlash(path), err)
		}
		if err := walkRefs(value, func(ref string) error {
			return validateRef(apiRoot, filepath.Dir(path), ref)
		}); err != nil {
			return fmt.Errorf("%s: %w", filepath.ToSlash(path), err)
		}
		return nil
	})
}

func walkRefs(value any, visit func(string) error) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == "$ref" || key == "$dynamicRef" || key == "$recursiveRef" {
				ref, ok := child.(string)
				if !ok {
					return fmt.Errorf("%s must be a string", key)
				}
				if err := visit(ref); err != nil {
					return err
				}
			}
			if err := walkRefs(child, visit); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := walkRefs(child, visit); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRef(apiRoot, containingDirectory, ref string) error {
	parsed, err := url.Parse(ref)
	if err != nil {
		return fmt.Errorf("invalid $ref %q: %w", ref, err)
	}
	if parsed.Scheme != "" || parsed.Host != "" || parsed.User != nil {
		return fmt.Errorf("non-local $ref %q is forbidden", ref)
	}
	pathPart := parsed.Path
	if pathPart == "" {
		return nil
	}
	if strings.HasPrefix(pathPart, "/") || strings.HasPrefix(pathPart, `\\`) || strings.Contains(pathPart, `\`) {
		return fmt.Errorf("absolute or platform-dependent $ref %q is forbidden", ref)
	}
	clean := filepath.Clean(filepath.FromSlash(pathPart))
	if filepath.VolumeName(clean) != "" {
		return fmt.Errorf("volume-qualified $ref %q is forbidden", ref)
	}
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || filepath.IsAbs(clean) {
		return fmt.Errorf("$ref %q escapes its containing directory", ref)
	}
	target := filepath.Join(containingDirectory, clean)
	if err := ensureWithin(apiRoot, target); err != nil {
		return fmt.Errorf("$ref %q: %w", ref, err)
	}
	if info, err := os.Stat(target); err != nil {
		return fmt.Errorf("$ref %q target: %w", ref, err)
	} else if info.IsDir() {
		return fmt.Errorf("$ref %q target is a directory", ref)
	}
	return nil
}

func resolveContractPath(root, base, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) || strings.Contains(relative, `\`) {
		return "", fmt.Errorf("unsafe relative path %q", relative)
	}
	clean := filepath.Clean(filepath.FromSlash(relative))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe relative path %q", relative)
	}
	basePath := filepath.Join(root, base)
	path := filepath.Join(basePath, clean)
	if err := ensureWithin(basePath, path); err != nil {
		return "", err
	}
	if info, err := os.Stat(path); err != nil {
		return "", err
	} else if info.IsDir() {
		return "", fmt.Errorf("path %q is a directory", relative)
	}
	return path, nil
}

func ensureWithin(root, path string) error {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return fmt.Errorf("path %q escapes %q", path, root)
	}
	return nil
}

func readJSONValue(path string) (any, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return jsonschema.UnmarshalJSON(file)
}

func readStrictJSON(path string, destination any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}
