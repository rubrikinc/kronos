package checksumfile

import (
	"bytes"
	"hash"
)

func valid(checksum []byte, data []byte, hash hash.Hash) bool {
	hash.Reset()
	if _, err := hash.Write(data); err != nil {
		return false
	}
	cksm := hash.Sum(nil)
	return bytes.Equal(checksum, cksm)
}

func computeHash(data []byte, hash hash.Hash) ([]byte, error) {
	hash.Reset()
	if _, err := hash.Write(data); err != nil {
		return nil, err
	}
	cksm := hash.Sum(nil)
	return cksm, nil
}
