// Package api holds the wire types and the operation whitelist shared by
// Gatekeeper, Trusted Host and Proxy, so every hop validates against the
// exact same definitions.
package api

import "encoding/json"

// OpKind tells the Proxy whether an operation must go to the primary.
type OpKind string

const (
	KindRead  OpKind = "read"
	KindWrite OpKind = "write"
)

// Policy is the Proxy routing policy requested by the client.
type Policy string

const (
	PolicyDirect  Policy = "direct"
	PolicyRandom  Policy = "random"
	PolicyLatency Policy = "latency"
)

// ParsePolicy returns the policy for s, defaulting to direct when s is empty.
func ParsePolicy(s string) (Policy, bool) {
	switch p := Policy(s); p {
	case "":
		return PolicyDirect, true
	case PolicyDirect, PolicyRandom, PolicyLatency:
		return p, true
	default:
		return "", false
	}
}

// ExecuteRequest is the internal message sent on every hop after the
// Gatekeeper: Gatekeeper -> Trusted Host -> Proxy, as POST /v1/execute.
type ExecuteRequest struct {
	RequestID string          `json:"request_id"`
	Op        string          `json:"op"`
	Kind      OpKind          `json:"kind"`
	Policy    Policy          `json:"policy"`
	Params    json.RawMessage `json:"params"`
}

// ExecuteResponse is what the Proxy returns (relayed unchanged to the client).
// Node/Role let the load generator count which database served each request.
type ExecuteResponse struct {
	RequestID    string           `json:"request_id"`
	Node         string           `json:"node"`
	Role         string           `json:"role"` // "primary" | "replica"
	Kind         OpKind           `json:"kind"`
	Policy       Policy           `json:"policy"`
	Fallback     bool             `json:"fallback,omitempty"` // read sent to primary because no replica was healthy
	DBMs         float64          `json:"db_ms"`
	Rows         []map[string]any `json:"rows,omitempty"`
	RowsAffected *int64           `json:"rows_affected,omitempty"`
	LastInsertID *int64           `json:"last_insert_id,omitempty"`
}
