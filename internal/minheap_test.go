package internal

import (
	"sync"
	"testing"
)

func TestMinHeap(t *testing.T) {
	mheap := InitMsgMinHeap(50)

	var wg sync.WaitGroup
	for j := 0; j < 100; j++ {
		wg.Add(1)
		go func(wgp *sync.WaitGroup) {
			defer wgp.Done()
			for i := 100; i > 0; i-- {
				mheap.Push(&Msg{occurs: i})
			}
		}(&wg)
	}
	wg.Wait()

	for mheap.Len() > 0 {
		a := mheap.Pop().occurs
		if 100 != a {
			t.Errorf("min heap error, should get 100 but %d", a)
		}
	}
}
