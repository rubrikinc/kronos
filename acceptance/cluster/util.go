package cluster

import (
	"go/build"
	"path/filepath"

	"github.com/pkg/errors"
)

func absoluteBinaryPath(binaryName string) (string, error) {
	root, err := build.Import("github.com/rubrikinc/kronos", "", build.FindOnly)
	if err != nil {
		return "", errors.Errorf("must run from within the kronos repository: %s", err)
	}
	return filepath.Join(
		root.Dir,
		binaryName,
	), nil
}
