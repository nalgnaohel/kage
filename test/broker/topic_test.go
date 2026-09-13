package broker_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nalgnaohel/kage/broker"
	"github.com/nalgnaohel/kage/storage"
)

func TestCreateTopic_CreatesOnePartitionPerNumPartitions(t *testing.T) {
	reg := newTestRegistry(t)

	err := reg.CreateTopic("orders", 3, 1)

	_, p0Ok := reg.GetLog("orders", 0)
	_, p1Ok := reg.GetLog("orders", 1)
	_, p2Ok := reg.GetLog("orders", 2)

	assertGolden(t, "topic", struct {
		Err        string
		Partitions []bool
	}{errString(err), []bool{p0Ok, p1Ok, p2Ok}})
}

func TestCreateTopic_AlreadyExists_ReturnsError(t *testing.T) {
	reg := newTestRegistry(t)

	firstErr := reg.CreateTopic("orders", 1, 1)
	secondErr := reg.CreateTopic("orders", 1, 1)

	assertGolden(t, "topic", struct {
		FirstErr  string
		SecondErr string
	}{errString(firstErr), errString(secondErr)})
}

func TestCreateTopic_InvalidNumPartitions_ReturnsError(t *testing.T) {
	reg := newTestRegistry(t)

	err := reg.CreateTopic("orders", 0, 1)

	assertGolden(t, "topic", struct{ Err string }{errString(err)})
}

func TestCreateTopic_InvalidReplicationFactor_ReturnsError(t *testing.T) {
	reg := newTestRegistry(t)

	err := reg.CreateTopic("orders", 1, 0)

	assertGolden(t, "topic", struct{ Err string }{errString(err)})
}

func TestListTopics_ReturnsSortedNames(t *testing.T) {
	reg := newTestRegistry(t)

	for _, topic := range []string{"orders", "clicks", "billing"} {
		if err := reg.CreateTopic(topic, 1, 1); err != nil {
			t.Fatalf("CreateTopic(%s) failed: %v", topic, err)
		}
	}

	assertGolden(t, "topic", struct{ Topics []string }{reg.ListTopics()})
}

func TestListTopics_NoTopics_ReturnsEmpty(t *testing.T) {
	reg := newTestRegistry(t)

	assertGolden(t, "topic", struct{ Topics []string }{reg.ListTopics()})
}

func TestDescribeTopic_ReturnsConfigAndPartitions(t *testing.T) {
	reg := newTestRegistry(t)

	if err := reg.CreateTopic("orders", 2, 1); err != nil {
		t.Fatalf("CreateTopic() failed: %v", err)
	}

	cfg, partitionIDs, found := reg.DescribeTopic("orders")

	assertGolden(t, "topic", struct {
		Found             bool
		NumPartitions     int32
		ReplicationFactor int32
		PartitionIDs      []int32
	}{found, cfg.NumPartitions, cfg.ReplicationFactor, partitionIDs})
}

func TestDescribeTopic_UnknownTopic_ReturnsNotFound(t *testing.T) {
	reg := newTestRegistry(t)

	_, _, found := reg.DescribeTopic("missing")

	assertGolden(t, "topic", struct{ Found bool }{found})
}

func TestDescribeTopic_AfterRestart_DefaultsReplicationFactorToOne(t *testing.T) {
	dir := t.TempDir()

	first := broker.NewRegistry(dir, storage.DefaultLogConfig(), "c1", 0, "localhost", 9093)
	if err := first.Startup(); err != nil {
		t.Fatalf("first Startup() failed: %v", err)
	}
	if err := first.CreateTopic("orders", 2, 1); err != nil {
		t.Fatalf("CreateTopic() failed: %v", err)
	}

	second := broker.NewRegistry(dir, storage.DefaultLogConfig(), "c1", 0, "localhost", 9093)
	if err := second.Startup(); err != nil {
		t.Fatalf("second Startup() failed: %v", err)
	}

	cfg, partitionIDs, found := second.DescribeTopic("orders")

	assertGolden(t, "topic", struct {
		Found             bool
		ReplicationFactor int32
		NumPartitionIDs   int
	}{found, cfg.ReplicationFactor, len(partitionIDs)})
}

func TestDeleteTopic_RemovesRegistryEntriesAndFiles(t *testing.T) {
	dir := t.TempDir()
	reg := broker.NewRegistry(dir, storage.DefaultLogConfig(), "c1", 0, "localhost", 9093)
	if err := reg.Startup(); err != nil {
		t.Fatalf("Startup() failed: %v", err)
	}
	if err := reg.CreateTopic("orders", 2, 1); err != nil {
		t.Fatalf("CreateTopic() failed: %v", err)
	}

	deleteErr := reg.DeleteTopic("orders")

	_, getLogOk := reg.GetLog("orders", 0)
	_, _, describeFound := reg.DescribeTopic("orders")

	var dirsExist []bool
	for _, p := range []int32{0, 1} {
		path := filepath.Join(dir, "orders-"+string(rune('0'+p)))
		_, err := os.Stat(path)
		dirsExist = append(dirsExist, err == nil)
	}

	assertGolden(t, "topic", struct {
		DeleteErr          string
		GetLogOk           bool
		DescribeFound      bool
		PartitionDirsExist []bool
	}{errString(deleteErr), getLogOk, describeFound, dirsExist})
}

func TestDeleteTopic_UnknownTopic_ReturnsError(t *testing.T) {
	reg := newTestRegistry(t)

	err := reg.DeleteTopic("missing")

	assertGolden(t, "topic", struct{ Err string }{errString(err)})
}
