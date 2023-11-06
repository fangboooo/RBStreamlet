package election

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/gitferry/bamboo/types"
	"github.com/stretchr/testify/require"
)

func TestRotation_IsLeader(t *testing.T) {
	elect := NewRotation(4)
	leaderID := elect.FindLeaderFor(1)
	require.True(t, elect.IsLeader(leaderID, 1))

	leaderID = elect.FindLeaderFor(4)
	require.Equal(t, "3", leaderID)

	leaderID = elect.FindLeaderFor(3)
	require.True(t, elect.IsLeader(leaderID, 3))
}

func TestRotation_LeaderList(t *testing.T) {
	elect := NewCsHRotation(4)
	menber := make(map[string]int)
	menber[strconv.Itoa(1)] = 2100
	menber[strconv.Itoa(2)] = 2100
	menber[strconv.Itoa(3)] = 2100
	menber[strconv.Itoa(4)] = 2100
	for i := 1; i <= 100; i++ {
		leaderID := elect.FindLeaderFor(types.View(i))
		fmt.Printf("view: %v, node id: %v\n", i, leaderID.Node())
	}
	menber[strconv.Itoa(3)] = 0
	elect.Hashr.UpdateWithWeights(menber)
	for i := 1; i <= 100; i++ {
		leaderID := elect.FindLeaderFor(types.View(i))
		fmt.Printf("view: %v, node id: %v\n", i, leaderID.Node())
	}
}
