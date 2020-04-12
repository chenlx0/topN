package internal

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
	"strconv"

	"github.com/chenlx0/topN/config"
	"github.com/chenlx0/topN/utils"
)

const (
	tmpFilePrefix = "topN-tmp"
)

// Msg represents each single string
type Msg struct {
	data   []byte
	hash   []byte
	offset int64
	occurs int
}

// GenMiddleFiles read source big file and split data into many small files
func GenMiddleFiles(conf *config.TopNConfig) error {
	// init channels
	msgChan := make(chan *Msg, 512)
	stopChan := make(chan int, conf.Concurrents)

	// start reduce tasks
	for i := 0; i < conf.Concurrents; i++ {
		go func() {
			if err := msgReduce(conf.SplitNum, conf.TmpFileDir, msgChan, stopChan); err != nil {
				log.Printf("reduce task error: %v", err)
			}
		}()
	}
	if err := msgMap(conf.SourceFile, msgChan, stopChan); err != nil {
		return err
	}
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
	for scanner.Scan() {
		nextMsg := &Msg{
			data:   scanner.Bytes(),
			offset: curOffset,
			occurs: 1,
		}
		curOffset += int64(len(nextMsg.data) + 1) // add 1 to count '\n'
		msgChan <- nextMsg
	}

	// to notify all reduce tasks exit
	for i := 0; i < len(stopChan); i++ {
		stopChan <- i
	}

	return nil
}

func msgReduce(splitSize int, tmpFileDir string, msgChan chan *Msg, stopChan chan int) error {
	toWrite := make(map[string]*Msg, 1024)
	count := 0

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
				if err := saveMiddleData(splitSize, toWrite); err != nil {
					return err
				}
			}
		case <-stopChan:
			if err := saveMiddleData(splitSize, toWrite); err != nil {
				return err
			}
			return nil
		}
	}
}

func saveMiddleData(splitSize int, middleData map[string]*Msg) error {
	offsetBytes := make([]byte, 8)
	occurBytes := make([]byte, 4)

	for _, v := range middleData {
		// convert offset and occur times to bytes, and added to temp file
		binary.BigEndian.PutUint64(offsetBytes, uint64(v.offset))
		binary.BigEndian.PutUint32(occurBytes, uint32(v.occurs))
		group := append(v.data, offsetBytes...)
		group = append(group, occurBytes...)
		group = append(group, byte('\n'))

		// open corresponding temp file and write
		fileName := tmpFilePrefix + strconv.Itoa(int(binary.BigEndian.Uint32(v.hash))%splitSize)
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
