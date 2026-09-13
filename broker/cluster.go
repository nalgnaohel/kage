package broker

// BrokerInfo mirrors kadmin.proto's BrokerInfo message.
type BrokerInfo struct {
	BrokerID int32
	Host     string
	Port     int32
}

// Always returns exactly this one broker until Raft/KRaft cluster
// membership exists (Phase 2).
func (r *Registry) GetClusterInfo() (clusterID string, brokers []BrokerInfo) {
	return r.clusterID, []BrokerInfo{{BrokerID: r.brokerID, Host: r.host, Port: r.port}}
}
