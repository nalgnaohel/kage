package raft

import (
	"context"
	"log"
	"time"

	"github.com/nalgnaohel/kage/broker"
)

const reconcileInterval = 5 * time.Second

type Reconciler struct {
	node     *Node
	registry *broker.Registry
}

func NewReconciler(node *Node, registry *broker.Registry) *Reconciler {
	return &Reconciler{node: node, registry: registry}
}

func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	r.reconcile()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.node.FSM().VersionCh():
			r.reconcile()
		case <-ticker.C:
			r.reconcile()
		}
	}
}

func (r *Reconciler) reconcile() {
	state := r.node.FSM().State()
	brokerID := r.node.BrokerID()

	for topic, meta := range state.Topics {
		for partition, assignment := range meta.Partitions {
			if !containsReplica(assignment.Replicas, brokerID) {
				continue
			}
			if _, ok := r.registry.GetLog(topic, partition); ok {
				continue
			}
			if _, err := r.registry.CreateLog(topic, partition); err != nil {
				log.Printf("raft: reconciler failed to create log for %s-%d: %v", topic, partition, err)
			}
		}
	}
}

func containsReplica(replicas []int32, brokerID int32) bool {
	for _, id := range replicas {
		if id == brokerID {
			return true
		}
	}
	return false
}
