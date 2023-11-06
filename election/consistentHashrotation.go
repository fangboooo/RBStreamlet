package election

import (
	"github.com/gitferry/bamboo/config"
	"github.com/gitferry/bamboo/identity"
	"github.com/gitferry/bamboo/types"
	"github.com/serialx/hashring"
	"math"
	"strconv"
)

type CsHrotation struct {
	Hashr       *hashring.HashRing
	peerNo      int
	k           int
	Weights     map[string]int
	Credibility map[string]float64
	propose     map[string]int
	commit      map[string]int
	num         int
}

func NewCsHRotation(peerNo int) *CsHrotation {
	memcacheServers := make([]string, 0)
	for i := 1; i <= peerNo; i++ {
		node := strconv.Itoa(i)
		memcacheServers = append(memcacheServers, node)
	}
	InitNum := 2100
	k := 3000
	weights := make(map[string]int)
	propose := make(map[string]int)
	commit := make(map[string]int)
	for i := 1; i <= peerNo; i++ {
		node := strconv.Itoa(i)
		weights[node] = InitNum
		propose[node] = 0
		commit[node] = 0
	}
	Credibility := make(map[string]float64)
	for i := 1; i <= peerNo; i++ {
		node := strconv.Itoa(i)
		Credibility[node] = float64(0.7)
	}
	ring := hashring.NewWithWeights(weights)
	num := 0
	return &CsHrotation{ring, peerNo, k, weights, Credibility, propose, commit, num}
}

func (r *CsHrotation) IsLeader(id identity.NodeID, view types.View) bool {
	node, _ := r.Hashr.GetNode(strconv.Itoa((int(view) - 1) / 3))
	return node == string(id)
}

func (r *CsHrotation) FindLeaderFor(view types.View) identity.NodeID {
	node, _ := r.Hashr.GetNode(strconv.Itoa((int(view) - 1) / 3))
	return identity.NodeID(node)
}

func (r *CsHrotation) UpdateWeight(newView types.View) {
	if r.num == 0 {
		r.num++
		byzno := config.GetConfig().ByzNo
		menber := make(map[string]int)
		for i := 1; i <= r.peerNo; i++ {
			node := strconv.Itoa(i)
			if i > byzno {
				menber[node] = 2100
			}
		}
		r.Hashr.UpdateWithWeights(menber)
	}
}

func (r *CsHrotation) UpdateBehaviour(comit bool, propose bool, id identity.NodeID) {
	if propose == false {
		r.commit[string(id)]++
	} else {
		r.propose[string(id)]++
	}
}

func PCR(CB int, AB int) float64 {
	if AB == 0 {
		return 0
	}

	return 3.0 * (float64(CB)/float64(AB) - 2.0/3.0)
}

func RP(C float64, MB int) float64 {
	if MB == 0 {
		return math.Cos(C * math.Pi / 2)
	}
	return -math.Sin(C * math.Pi / 2)
}

func Ci(cb int, pcr float64, rp float64, mb int, c float64) float64 {
	//k := -(1+math.Log(1+float64(cb))*pcr+2*rp) + 2.0*float64(mb)
	k := -(3*pcr + 2*rp) + 2.0*float64(mb)
	k1 := 1 + math.Exp(k)
	x := (5.0 / 10.0)
	y := 1.0 - x
	return x/(k1) + c*math.Pow((y), float64(mb)+1)
}
