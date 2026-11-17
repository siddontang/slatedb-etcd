package main

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func nativeBytes(value string) []byte {
	data := []byte(value)
	hash := sha256.Sum256(data)
	return append(data, hash[:]...)
}

func TestNativeExportPreservesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup.snapshot")
	original := nativeBytes("first native payload")
	require.NoError(t, saveSnapshot(bytes.NewReader(original), path, 1024))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	err = saveSnapshot(bytes.NewReader(nativeBytes("second payload")), path, 1024)
	require.ErrorIs(t, err, os.ErrExist)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, original, data)
}

type failedReader struct{}

func (failedReader) Read([]byte) (int, error) { return 0, errors.New("stream failed") }

func TestNativeExportRejectsDamagedStreams(t *testing.T) {
	corrupt := nativeBytes("payload")
	corrupt[0] ^= 1
	for name, reader := range map[string]io.Reader{
		"checksum":     bytes.NewReader(corrupt),
		"truncated":    bytes.NewReader([]byte("short")),
		"oversized":    bytes.NewReader(nativeBytes("a payload that exceeds the test limit")),
		"read_failure": failedReader{},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			err := saveSnapshot(reader, filepath.Join(dir, "backup.snapshot"), 64)
			require.Error(t, err)
			files, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Empty(t, files)
		})
	}
}
