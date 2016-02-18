package store

import "sync"

type Cluster struct {
	sync.RWMutex

	Nodes map[uint64]*RaftNode
}

func NewCluster() *Cluster {
	return &Cluster{
		Nodes: make(map[uint64]*RaftNode),
	}
}

// Add a node to our neighbors
func (t *Cluster) AddNode(node *RaftNode) {
	t.Lock()
	t.Nodes[node.ID] = node
	t.Unlock()
}

//Remove a node from our neighbors
func (t *Cluster) RemoveNode(id uint64) {
	t.Lock()
	delete(t.Nodes, id)
	t.Unlock()
}
