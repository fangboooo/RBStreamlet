package crypto

import (
	"fmt"
	"testing"
)

func TestStreamlet_CommitRule1(t *testing.T) {
	prevID := MakeID("Genesis block")
	fmt.Println(prevID)
}
