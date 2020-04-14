package internal

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/chenlx0/topN/config"
	"github.com/chenlx0/topN/utils"
)

const (
	tmpFilePrefix = "topN-tmp"
)

// GenMiddleFiles read source big file and split data into many small files
func GenMiddleFiles(conf *config.TopNConfig) error {
	// init channels
	msgChan := make(chan *Msg, 64)
	stopChan := make(chan int, conf.Concurrents)

	// start reduce tasks
	var wg sync.WaitGroup
	for i := 0; i < conf.Concurrents; i++ {
		wg.Add(1)
		go func(wgp *sync.WaitGroup) {
			defer wgp.Done()
			if err := msgReduce(conf.SplitNum, conf.TmpFileDir, msgChan, stopChan); err != nil {
				log.Printf("reduce task error: %v", err)
			}
		}(&wg)
	}
	if err := msgMap(conf.SourceFile, msgChan, stopChan); err != nil {
		return err
	}
	// to notify all reduce tasks exit
	for i := 0; i < conf.Concurrents; i++ {
		stopChan <- i
	}
	wg.Wait()
	return nil
}

func msgMap(filePath string, msgChan chan *Msg, stopChan chan int) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}

	curOffset := int64(0)
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	var b []byte
	for scanner.Scan() {
		b = scanner.Bytes()
		toSend := make([]byte, len(b))
		copy(toSend, b)
		nextMsg := &Msg{
			data:   toSend,
			offset: curOffset,
			occurs: 1,
		}
		curOffset += int64(len(nextMsg.data) + 1) // add 1 to count '\n'
		msgChan <- nextMsg
	}

	return nil
}

func msgReduce(splitSize int, tmpFileDir string, msgChan chan *Msg, stopChan chan int) error {
	toWrite := make(map[string]*Msg, 1024)
	count := 0
	i := 0

	for {
		var nextMsg *Msg
		select {
		case nextMsg = <-msgChan:
			nextMsg.hash = utils.Hash(nextMsg.data)
			hashStr := string(nextMsg.hash[:])
			if _, ok := toWrite[hashStr]; !ok {
				toWrite[hashStr] = nextMsg
				count++
			} else {
				toWrite[hashStr].occurs++
			}
			if count >= 1024 {
				if err := saveMiddleData(splitSize, tmpFileDir, toWrite); err != nil {
					return err
				}
				toWrite = make(map[string]*Msg, 1024)
				count = 0
			}
		case i = <-stopChan:
			if err := saveMiddleData(splitSize, tmpFileDir, toWrite); err != nil {
				return err
			}
			log.Printf("reduce task %d end", i)
			return nil
		}
	}
}

func saveMiddleData(splitSize int, tmpFileDir string, middleData map[string]*Msg) error {
	offsetBytes := make([]byte, 8)
	occurBytes := make([]byte, 4)

	for _, v := range middleData {
		// convert offset and occur times to bytes, and added to temp file
		binary.BigEndian.PutUint64(offsetBytes, uint64(v.offset))
		binary.BigEndian.PutUint32(occurBytes, uint32(v.occurs))
		group := append(v.hash, offsetBytes...)
		group = append(group, occurBytes...)

		// open corresponding temp file and write
		fileName := tmpFileDir + tmpFilePrefix + strconv.Itoa(int(binary.BigEndian.Uint32(v.hash))%splitSize)
		f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
		if err != nil {
			return err
		}
		if _, err := f.Write(group); err != nil {
			f.Close()
			return err
		}
		f.Close()
	}
	return nil
}
