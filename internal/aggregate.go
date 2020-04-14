package internal

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/chenlx0/topN/config"
	"github.com/chenlx0/topN/utils"
)

const (
	offsetSize = 8
	occursSize = 4
	lineSize   = utils.HashSize + offsetSize + occursSize
)

// Aggregate aggregate all middle file data to a heap,
// and convert to slice and return
func Aggregate(conf *config.TopNConfig) ([]*Msg, error) {
	fileNumber := int32(conf.SplitNum)
	mheap := InitMsgMinHeap(conf.N)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(wgp *sync.WaitGroup) {
			defer wgp.Done()
			if err := singleAggregate(conf.TmpFileDir, &fileNumber, mheap, conf.N); err != nil {
				log.Printf("single aggregate failed: %v\n", err)
			}
		}(&wg)
	}
	wg.Wait()

	res := make([]*Msg, 0)
	for mheap.Len() > 0 {
		res = append(res, mheap.Pop())
	}
	return res, nil
}

func singleAggregate(tmpFileDir string, curFileNumber *int32, mheap *MsgMinHeap, maxHeapSize int) error {

	// line size = sizeof(hash value) + sizeof(offset) + sizeof(occurs) + sizeof('\n')
	msgBytes := make([]byte, lineSize)
	for {
		newNumber := atomic.AddInt32(curFileNumber, -1)
		if newNumber < 0 {
			// curFileNumber < 0 means we have searched all temp files
			return nil
		}

		n := lineSize
		tmpFilePath := tmpFileDir + tmpFilePrefix + strconv.Itoa(int(newNumber))
		hashMsgMap := make(map[string]*Msg, 0)
		f, err := os.OpenFile(tmpFilePath, os.O_CREATE|os.O_RDONLY, 0755)
		if err != nil {
			return err
		}
		defer f.Close()
		reader := bufio.NewReader(f)
		for {
			n, err = io.ReadFull(reader, msgBytes)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			hashStr := string(msgBytes[0:utils.HashSize])
			offset := int64(binary.BigEndian.Uint64(msgBytes[utils.HashSize : utils.HashSize+offsetSize]))
			occurs := int(binary.BigEndian.Uint32(msgBytes[utils.HashSize+offsetSize : utils.HashSize+offsetSize+occursSize]))
			if _, ok := hashMsgMap[hashStr]; !ok {
				hashMsgMap[hashStr] = &Msg{
					offset: offset,
					occurs: occurs,
				}
			} else {
				hashMsgMap[hashStr].occurs += occurs
			}
		}
		// aggregate map data to heap
		for _, v := range hashMsgMap {
			mheap.Push(v)
		}
	}
}
