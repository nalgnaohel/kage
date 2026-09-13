package broker

// BrokerInfo identifies a single broker in the cluster (mirrors
// kadmin.proto's BrokerInfo message).
type BrokerInfo struct {
	BrokerID int32
	Host     string
	Port     int32
}

// GetClusterInfo returns the cluster ID and every known broker. Phase 1 is
// single-broker, so this always returns exactly one BrokerInfo — this
// process itself. Once Raft/KRaft controller membership exists (Phase 2),
// this will report every broker registered with the cluster instead.
func (r *Registry) GetClusterInfo() (clusterID string, brokers []BrokerInfo) {
	return r.clusterID, []BrokerInfo{{BrokerID: r.brokerID, Host: r.host, Port: r.port}}
}
