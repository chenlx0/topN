package internal

import (
	"container/heap"
	"sync"
)

// MsgMinHeap is a customized, size fixed min heap
// we use it to get Msg with biggest occurs
type MsgMinHeap struct {
	mMinHeap *minHeap
	mux      sync.Mutex
	maxSize  int
}

// minHeap implements container/heap interface
type minHeap []*Msg

// Implement heap.Interface for minHeap

func (mh minHeap) Len() int {
	return len(mh)
}

func (mh minHeap) Swap(i, j int) {
	mh[i], mh[j] = mh[j], mh[i]
}

func (mh minHeap) Less(i, j int) bool {
	return mh[i].occurs > mh[j].occurs
}

func (mh *minHeap) Push(h interface{}) {
	*mh = append(*mh, h.(*Msg))
}

func (mh *minHeap) Pop() interface{} {
	n := len(*mh)
	if n == 0 {
		return nil
	}
	x := (*mh)[0]
	*mh = (*mh)[1:]
	return x
}

// InitMsgMinHeap init and return MsgMinHeap
func InitMsgMinHeap(maxSize int) *MsgMinHeap {
	mhp := &minHeap{&Msg{occurs: 0}}
	heap.Init(mhp)
	res := &MsgMinHeap{
		mMinHeap: mhp,
		maxSize:  maxSize,
	}
	return res
}

// Push a Msg to our custom heap
// if heap size is bigger than the maxSize, then pop the element in the top
func (mmh *MsgMinHeap) Push(m *Msg) {
	mmh.mux.Lock()
	if mmh.mMinHeap.Len() >= mmh.maxSize {
		mmh.mMinHeap.Pop()
	}
	mmh.mMinHeap.Push(m)
	mmh.mux.Unlock()
}

// Pop a Msg from our custom heap
func (mmh *MsgMinHeap) Pop() *Msg {
	mmh.mux.Lock()
	res := mmh.mMinHeap.Pop()
	mmh.mux.Unlock()
	return res.(*Msg)
}

// Top return smallest element in heap
func (mmh *MsgMinHeap) Top() *Msg {
	mmh.mux.Lock()
	res := (*mmh.mMinHeap)[0]
	mmh.mux.Unlock()
	return res
}

func (mmh *MsgMinHeap) Len() int {
	mmh.mux.Lock()
	res := mmh.mMinHeap.Len()
	mmh.mux.Unlock()
	return res
}
