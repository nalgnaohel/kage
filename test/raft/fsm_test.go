package raft_test

import (
	"testing"

	hraft "github.com/hashicorp/raft"
	"github.com/nalgnaohel/kage/raft"
)

func TestFSM_Apply_RegisterBrokersThenCreateTopic(t *testing.T) {
	fsm := raft.NewFSM()

	registerCmd1, err := raft.NewRegisterBrokerCommand(raft.RegisterBrokerCommand{
		BrokerID: 1, Host: "localhost", Port: 9092, RaftAddr: "127.0.0.1:9094",
	})
	if err != nil {
		t.Fatalf("NewRegisterBrokerCommand() failed: %v", err)
	}
	registerCmd2, err := raft.NewRegisterBrokerCommand(raft.RegisterBrokerCommand{
		BrokerID: 2, Host: "localhost", Port: 9093, RaftAddr: "127.0.0.1:9095",
	})
	if err != nil {
		t.Fatalf("NewRegisterBrokerCommand() failed: %v", err)
	}
	createCmd, err := raft.NewCreateTopicCommand(raft.CreateTopicCommand{
		Topic: "orders", NumPartitions: 3, ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("NewCreateTopicCommand() failed: %v", err)
	}

	resp1 := apply(t, fsm, registerCmd1)
	resp2 := apply(t, fsm, registerCmd2)
	resp3 := apply(t, fsm, createCmd)

	assertGolden(t, "fsm", struct {
		Resp1Err string
		Resp2Err string
		Resp3Err string
		State    raft.State
	}{respErrString(resp1), respErrString(resp2), respErrString(resp3), fsm.State()})
}

func TestFSM_Apply_CreateTopic_Duplicate_ReturnsErrorWithoutMutatingState(t *testing.T) {
	fsm := raft.NewFSM()

	createCmd, err := raft.NewCreateTopicCommand(raft.CreateTopicCommand{
		Topic: "orders", NumPartitions: 1, ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("NewCreateTopicCommand() failed: %v", err)
	}

	firstResp := apply(t, fsm, createCmd)
	secondResp := apply(t, fsm, createCmd)

	assertGolden(t, "fsm", struct {
		FirstErr  string
		SecondErr string
		State     raft.State
	}{respErrString(firstResp), respErrString(secondResp), fsm.State()})
}

func TestFSM_Apply_DeleteTopic_UnknownTopic_ReturnsError(t *testing.T) {
	fsm := raft.NewFSM()

	deleteCmd, err := raft.NewDeleteTopicCommand(raft.DeleteTopicCommand{Topic: "missing"})
	if err != nil {
		t.Fatalf("NewDeleteTopicCommand() failed: %v", err)
	}

	resp := apply(t, fsm, deleteCmd)

	assertGolden(t, "fsm", struct {
		RespErr string
		State   raft.State
	}{respErrString(resp), fsm.State()})
}

func TestFSM_Apply_CreateThenDeleteTopic(t *testing.T) {
	fsm := raft.NewFSM()

	createCmd, err := raft.NewCreateTopicCommand(raft.CreateTopicCommand{
		Topic: "orders", NumPartitions: 1, ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("NewCreateTopicCommand() failed: %v", err)
	}
	deleteCmd, err := raft.NewDeleteTopicCommand(raft.DeleteTopicCommand{Topic: "orders"})
	if err != nil {
		t.Fatalf("NewDeleteTopicCommand() failed: %v", err)
	}

	createResp := apply(t, fsm, createCmd)
	deleteResp := apply(t, fsm, deleteCmd)

	assertGolden(t, "fsm", struct {
		CreateErr string
		DeleteErr string
		State     raft.State
	}{respErrString(createResp), respErrString(deleteResp), fsm.State()})
}

func TestFSM_Apply_UnknownCommandType_ReturnsError(t *testing.T) {
	fsm := raft.NewFSM()

	data, err := raft.Encode(raft.Command{Type: "Bogus"})
	if err != nil {
		t.Fatalf("Encode() failed: %v", err)
	}
	resp := fsm.Apply(&hraft.Log{Data: data})

	assertGolden(t, "fsm", struct {
		RespErr string
		State   raft.State
	}{respErrString(resp), fsm.State()})
}

func TestFSM_SnapshotRestore_RoundTrip(t *testing.T) {
	fsm := raft.NewFSM()

	registerCmd, err := raft.NewRegisterBrokerCommand(raft.RegisterBrokerCommand{
		BrokerID: 1, Host: "localhost", Port: 9092, RaftAddr: "127.0.0.1:9094",
	})
	if err != nil {
		t.Fatalf("NewRegisterBrokerCommand() failed: %v", err)
	}
	createCmd, err := raft.NewCreateTopicCommand(raft.CreateTopicCommand{
		Topic: "orders", NumPartitions: 3, ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("NewCreateTopicCommand() failed: %v", err)
	}
	apply(t, fsm, registerCmd)
	apply(t, fsm, createCmd)

	store := hraft.NewInmemSnapshotStore()
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() failed: %v", err)
	}
	sink, err := store.Create(1, 0, 0, hraft.Configuration{}, 0, nil)
	if err != nil {
		t.Fatalf("store.Create() failed: %v", err)
	}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist() failed: %v", err)
	}

	_, rc, err := store.Open(sink.ID())
	if err != nil {
		t.Fatalf("store.Open() failed: %v", err)
	}

	restored := raft.NewFSM()
	if err := restored.Restore(rc); err != nil {
		t.Fatalf("Restore() failed: %v", err)
	}

	assertGolden(t, "fsm", struct {
		Original raft.State
		Restored raft.State
	}{fsm.State(), restored.State()})
}
