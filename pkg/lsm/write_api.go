package lsm

import "time"

// WriteProvider exposes data write APIs to external transports.
type WriteProvider interface {
	Put(key []byte, value []byte) error
	Delete(key []byte) error
}

// WriteConsistency controls acknowledgement semantics for write requests.
type WriteConsistency string

const (
	// WriteConsistencyAccepted acknowledges once the request is admitted.
	WriteConsistencyAccepted WriteConsistency = "accepted"
	// WriteConsistencyLocalCommitted acknowledges after local commit and apply.
	WriteConsistencyLocalCommitted WriteConsistency = "local_committed"
)

// WriteRequestState represents write lifecycle state for async tracking.
type WriteRequestState string

const (
	WriteRequestPending   WriteRequestState = "pending"
	WriteRequestCommitted WriteRequestState = "committed"
	WriteRequestRejected  WriteRequestState = "rejected"
)

// WriteRequestStatus reports lifecycle state for a write request.
type WriteRequestStatus struct {
	RequestID   string            `json:"request_id"`
	Operation   string            `json:"operation"`
	Consistency WriteConsistency  `json:"consistency"`
	State       WriteRequestState `json:"state"`
	Error       string            `json:"error,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}
