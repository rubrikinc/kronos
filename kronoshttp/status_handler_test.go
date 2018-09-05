package kronoshttp

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatusHandlerServeHTTP(t *testing.T) {
	a := assert.New(t)
	nodeID := "1"
	sh := NewStatusHandler(nodeID)
	req, err := http.NewRequest(http.MethodGet, "", nil)
	a.NoError(err)
	rr := httptest.NewRecorder()
	sh.ServeHTTP(rr, req)
	rawNodeID, err := ioutil.ReadAll(rr.Body)
	a.NoError(err)
	a.Equal(nodeID, string(rawNodeID))
}
