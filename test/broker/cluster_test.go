package broker_test

import (
	"testing"

	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/storage"
)

func TestGetClusterInfo_ReturnsSingleBroker(t *testing.T) {
	dir := t.TempDir()
	reg := broker.NewRegistry(dir, storage.DefaultLogConfig(), "my-cluster", 7, "example.com", 9093)
	if err := reg.Startup(); err != nil {
		t.Fatalf("Startup() failed: %v", err)
	}

	clusterID, brokers := reg.GetClusterInfo()

	assertGolden(t, "cluster", struct {
		ClusterID string
		Brokers   []broker.BrokerInfo
	}{clusterID, brokers})
}
