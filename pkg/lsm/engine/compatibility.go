package engine

const (
	clusterStatusCompatibilityVersion   = 1
	raftPeerMessageCompatibilityVersion = 1
)

// CompatibilityStatus reports LSMEngine-owned wire and persisted contract
// versions that operators can compare during rolling upgrades.
type CompatibilityStatus struct {
	ClusterStatusVersion   int `json:"cluster_status_version"`
	ControlStateVersion    int `json:"control_state_version"`
	StateSnapshotVersion   int `json:"state_snapshot_version"`
	RaftPeerMessageVersion int `json:"raft_peer_message_version"`
}

func currentCompatibilityStatus() CompatibilityStatus {
	return CompatibilityStatus{
		ClusterStatusVersion:   clusterStatusCompatibilityVersion,
		ControlStateVersion:    currentControlStateVersion,
		StateSnapshotVersion:   lsmStateSnapshotVersion,
		RaftPeerMessageVersion: raftPeerMessageCompatibilityVersion,
	}
}
