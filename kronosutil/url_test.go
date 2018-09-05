package kronosutil

import (
	"net/url"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/scaledata/kronos/pb"
)

func TestAddrToURL(t *testing.T) {
	testCases := []struct {
		name        string
		addr        *kronospb.NodeAddr
		secure      bool
		urlInString string
	}{
		{
			name: "secure",
			addr: &kronospb.NodeAddr{
				Host: "127.0.0.1",
				Port: "1",
			},
			secure:      true,
			urlInString: "https://127.0.0.1:1",
		},
		{
			name: "insecure",
			addr: &kronospb.NodeAddr{
				Host: "127.0.0.1",
				Port: "1",
			},
			secure:      false,
			urlInString: "http://127.0.0.1:1",
		},
	}
	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				a := assert.New(t)
				url := AddrToURL(tc.addr, tc.secure)
				a.Equal(tc.urlInString, url.String())
			},
		)
	}
}

func TestResolveURL(t *testing.T) {
	testCases := []struct {
		name        string
		baseURL     string
		path        string
		resolvedURL string
	}{
		{
			name:        "empty path",
			baseURL:     "https://127.0.0.1/abc",
			path:        "",
			resolvedURL: "https://127.0.0.1/abc",
		},
		{
			name:        "non-empty path",
			baseURL:     "https://127.0.0.1/abc",
			path:        "def",
			resolvedURL: "https://127.0.0.1/abc/def",
		},
	}
	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				a := assert.New(t)
				baseURL, err := url.Parse(tc.baseURL)
				a.NoError(err)
				resolvedURL := AddToURLPath(*baseURL, tc.path)
				a.Equal(tc.resolvedURL, resolvedURL.String())
			},
		)
	}
}

func TestNodeAddr(t *testing.T) {
	testCases := []struct {
		name      string
		addr      string
		nodeAddr  *kronospb.NodeAddr
		errRegexp *regexp.Regexp
	}{
		{
			name:     "valid",
			addr:     "localhost:5766",
			nodeAddr: &kronospb.NodeAddr{Host: "localhost", Port: "5766"},
		},
		{
			name:      "invalid",
			addr:      "localhost",
			errRegexp: regexp.MustCompile("missing port"),
		},
	}
	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				a := assert.New(t)
				nodeAddr, err := NodeAddr(tc.addr)
				if tc.errRegexp != nil {
					a.Regexp(tc.errRegexp, err.Error())
				} else {
					a.NoError(err)
				}
				a.Equal(tc.nodeAddr, nodeAddr)
			},
		)
	}

}
