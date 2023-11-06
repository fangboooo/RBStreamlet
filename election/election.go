package election

import (
	"github.com/gitferry/bamboo/identity"
	"github.com/gitferry/bamboo/types"
)

type Election interface {
	IsLeader(id identity.NodeID, view types.View) bool
	FindLeaderFor(view types.View) identity.NodeID
	UpdateWeight(newView types.View)
	UpdateBehaviour(comit bool, propose bool, id identity.NodeID)
}
