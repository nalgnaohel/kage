package raft

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const (
	transportMaxPool   = 3
	transportTimeout   = 10 * time.Second
	snapshotRetain     = 2
	defaultProposeWait = 10 * time.Second
)

type Config struct {
	BrokerID      int32
	BindAddr      string
	AdvertiseAddr string
	DataDir       string
}

type Node struct {
	raft      *hraft.Raft
	fsm       *FSM
	brokerID  int32
	transport *hraft.NetworkTransport
}

type ErrNotLeader struct {
	LeaderID   int32
	LeaderAddr string
}

func (e *ErrNotLeader) Error() string {
	return fmt.Sprintf("raft: not leader; leader is broker %d at %s", e.LeaderID, e.LeaderAddr)
}

func NewNode(cfg Config) (*Node, error) {
	advertiseAddr, err := net.ResolveTCPAddr("tcp", cfg.AdvertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("raft: resolve advertise addr: %w", err)
	}

	transport, err := hraft.NewTCPTransport(cfg.BindAddr, advertiseAddr, transportMaxPool, transportTimeout, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft: new transport: %w", err)
	}

	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: mkdir %s: %w", raftDir, err)
	}

	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(raftDir, "raft.db")})
	if err != nil {
		return nil, fmt.Errorf("raft: new bolt store: %w", err)
	}

	snapshots, err := hraft.NewFileSnapshotStore(filepath.Join(raftDir, "snapshots"), snapshotRetain, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft: new snapshot store: %w", err)
	}

	fsm := NewFSM()

	raftConfig := hraft.DefaultConfig()
	raftConfig.LocalID = hraft.ServerID(strconv.Itoa(int(cfg.BrokerID)))

	r, err := hraft.NewRaft(raftConfig, fsm, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("raft: new raft: %w", err)
	}

	return &Node{
		raft:      r,
		fsm:       fsm,
		brokerID:  cfg.BrokerID,
		transport: transport,
	}, nil
}

func (n *Node) Bootstrap() error {
	config := hraft.Configuration{
		Servers: []hraft.Server{
			{
				ID:      hraft.ServerID(strconv.Itoa(int(n.brokerID))),
				Address: n.transport.LocalAddr(),
			},
		},
	}
	return n.raft.BootstrapCluster(config).Error()
}

func (n *Node) Propose(cmd Command, timeout time.Duration) (interface{}, error) {
	if n.raft.State() != hraft.Leader {
		return nil, n.notLeaderErr()
	}

	data, err := Encode(cmd)
	if err != nil {
		return nil, err
	}

	if timeout <= 0 {
		timeout = defaultProposeWait
	}

	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return nil, err
	}

	resp := future.Response()
	if respErr, ok := resp.(error); ok && respErr != nil {
		return nil, respErr
	}
	return resp, nil
}

func (n *Node) notLeaderErr() error {
	addr, id := n.raft.LeaderWithID()
	leaderID, _ := strconv.Atoi(string(id))
	return &ErrNotLeader{LeaderID: int32(leaderID), LeaderAddr: string(addr)}
}

func (n *Node) FSM() *FSM {
	return n.fsm
}

func (n *Node) BrokerID() int32 {
	return n.brokerID
}

func (n *Node) Raft() *hraft.Raft {
	return n.raft
}
