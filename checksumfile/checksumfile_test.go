package checksumfile

import (
	"bytes"
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/assert"

	"github.com/scaledata/kronos/kronosutil/log"
)

const (
	numBytes = 50
)

func randomBytes(n int) []byte {
	p := make([]byte, n)
	rand.Read(p)
	return p
}

func testShouldReadLatestData(filename string, numIterations int) func(*testing.T) {
	return func(t *testing.T) {
		countFaulty := 0
		for i := 0; i < numIterations; i++ {
			data := randomBytes(numBytes)
			if Write(filename, data) != nil {
				countFaulty++
			}
			read, err := Read(filename)
			if err != nil || !bytes.Equal(data, read) {
				countFaulty++
			}
		}
		if countFaulty > 0 {
			t.Fatalf("%d reads/writes out of %d failed", countFaulty, numIterations)
		}
	}
}

func TestHandleWriteRead(t *testing.T) {
	dir, err := ioutil.TempDir("", "checksumfile")
	assert.Nil(t, err)
	filename := filepath.Join(dir, "TestBasic")
	assert.Nil(t, err)
	testShouldReadLatestData(filename, 50)(t)
}

func TestReadChecksumMismatch(t *testing.T) {
	dir, err := ioutil.TempDir("", "checksumfile")
	assert.Nil(t, err)
	filename := filepath.Join(dir, "TestBasic")
	p := randomBytes(numBytes)
	cksm := randomBytes(16)
	fe, err := protoutil.Marshal(&FileExtent{Checksum: cksm, Data: p})
	assert.Nil(t, err)
	assert.Nil(t, ioutil.WriteFile(filename, fe, 0644))
	_, err = Read(filename)
	assert.Regexp(t, "could not read from file.*checksum and data don't match", err.Error())
}

func TestReadWithoutWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "checksumfile")
	assert.Nil(t, err)
	filename := filepath.Join(dir, "TestBasic")
	_, err = Read(filename)
	assert.Regexp(t, "could not read from file.*no such file or directory", err.Error())
}

func TestDirectoryDoesNotExist(t *testing.T) {
	filename := filepath.Join("TestBasic", "/dir_does_not_exist/")
	err := Write(filename, randomBytes(numBytes))
	assert.Regexp(t, "no such file or directory", err.Error())
}

func TestDirectoryWrongPermission(t *testing.T) {
	dir, err := ioutil.TempDir("", "checksumfile")
	assert.Nil(t, err)
	if err := os.Chmod(dir, 0512); err != nil {
		log.Fatal(context.TODO(), err)
	}
	filename := filepath.Join(dir, "TestBasic")
	err = Write(filename, randomBytes(numBytes))
	assert.Regexp(t, "permission denied", err.Error())
}
