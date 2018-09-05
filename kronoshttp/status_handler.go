package kronoshttp

import (
	"fmt"
	"net/http"
)

// StatusHandler is used to check whether the raft HTTP server is up. It
// returns the raft id of the node on GET calls.
type StatusHandler struct {
	// nodeID is node's raft ID.
	nodeID string
}

// NewStatusHandler creates a statusHandler, that responds with nodeID on GET
// requests.
func NewStatusHandler(nodeID string) *StatusHandler {
	return &StatusHandler{nodeID: nodeID}
}

// ServeHTTP returns whether the raft HTTP server is up
func (h *StatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		fmt.Fprintf(w, h.nodeID)
	default:
		fmt.Fprintf(w, "Only GET method is supported")
	}
}
