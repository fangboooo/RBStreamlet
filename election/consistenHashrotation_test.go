package election

import (
	"fmt"
	"github.com/gitferry/bamboo/types"
	"testing"
)

func TestCsHRotation_IsLeader(t *testing.T) {
	elect := NewCsHRotation(4)
	count := make(map[string]int)
	for i := 1; i <= 20; i++ {
		node := elect.FindLeaderFor(types.View(i))
		count[string(node)]++
	}
	fmt.Println(count)
}
