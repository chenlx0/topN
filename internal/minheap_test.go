package internal

import (
	"testing"
)

func TestMinHeap(t *testing.T) {
	mheap := InitMsgMinHeap(50)

	for i := 0; i < 1000; i++ {
		mheap.Push(&Msg{occurs: i})
	}

	for mheap.Len() > 0 {
		t.Logf("%d\n", mheap.Pop().occurs)
	}
}
