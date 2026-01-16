// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package datanode

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIsMountPoint(t *testing.T) {
	// Root directory behavior depends on the host FS layout.
	rootResult := isMountPoint("/")
	t.Logf("IsMountPoint(/) = %v (system-dependent)", rootResult)

	// Temporary dir should NOT be a mount point.
	tempDir, err := os.MkdirTemp("", "cubefs-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	if isMountPoint(tempDir) {
		t.Errorf("Expected %s to NOT be a mount point", tempDir)
	}

	// Non-existent path should return false.
	if isMountPoint("/non/existent/path") {
		t.Errorf("Expected non-existent path to return false")
	}

	// Symlink should NOT be a mount point.
	symlinkPath := filepath.Join(tempDir, "symlink")
	if err := os.Symlink("/tmp", symlinkPath); err != nil {
		t.Logf("Warning: Failed to create symlink: %v", err)
	} else if isMountPoint(symlinkPath) {
		t.Errorf("Expected symlink to NOT be a mount point")
	}

	// Common mount points on Linux.
	commonMountPoints := []string{"/proc", "/sys", "/dev"}
	for _, mp := range commonMountPoints {
		if _, err := os.Stat(mp); err == nil {
			result := isMountPoint(mp)
			t.Logf("IsMountPoint(%s) = %v", mp, result)
		}
	}
}

func TestIsMountPointEdgeCases(t *testing.T) {
	if isMountPoint("") {
		t.Errorf("Expected empty string to return false")
	}

	// Relative path checks; may log if host FS mounts relative dirs.
	if isMountPoint(".") {
		t.Logf("Current directory is a mount point (host-dependent)")
	}
	if isMountPoint("..") {
		t.Logf("Parent directory is a mount point (host-dependent)")
	}
}
