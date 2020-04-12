package internal

import (
	"testing"
)

func TestMinHeap(t *testing.T) {
	mheap := InitMsgMinHeap()

	for i := 0; i < 100; i++ {
		mheap.Push(&Msg{occurs: i})
	}

	for mheap.Len() > 0 {
		t.Logf("%d\n", mheap.Pop().occurs)
	}
}
