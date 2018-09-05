package checksumfile

import (
	"crypto/md5"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidAndComputeHash(t *testing.T) {
	hash := md5.New()
	type testcase struct {
		data     []byte
		checksum []byte
	}
	cases := map[string]testcase{ // use map to verify order is irrelevant for a hash
		"1": {
			data:     []byte{1, 2, 3},
			checksum: []byte{82, 137, 223, 115, 125, 245, 115, 38, 252, 221, 34, 89, 122, 251, 31, 172},
		},
		"2": {
			data:     []byte("This is a big random string for which verifying checksum"),
			checksum: []byte{216, 204, 143, 238, 254, 87, 146, 48, 106, 52, 92, 124, 104, 207, 31, 22},
		},
		"3": {
			data:     []byte("This is another random string for checksum verification"),
			checksum: []byte{0x92, 0x97, 0xd5, 0xe2, 0x87, 0xbb, 0xd4, 0xcd, 0x90, 0xe1, 0x90, 0x9d, 0xf4, 0xde, 0x4b, 0xef},
		},
	}
	for _, tc := range cases {
		checksum, err := computeHash(tc.data, hash)
		assert.Nil(t, err)
		assert.Equal(t, tc.checksum, checksum)
	}
	for _, tc := range cases {
		assert.Equal(t, valid(tc.checksum, tc.data, hash), true)
	}
}
