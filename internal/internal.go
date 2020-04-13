package internal

import (
	"bufio"
	"os"
)

// Msg represents each single string
type Msg struct {
	data   []byte
	hash   []byte
	offset int64
	occurs int
}

func genMsgData(sourceFile string, msgList []*Msg) error {
	f, err := os.Open(sourceFile)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, v := range msgList {
		f.Seek(v.offset, 0)
		scanner := bufio.NewScanner(f)
		scanner.Split(bufio.ScanLines)
		v.data = scanner.Bytes()
	}
	return nil
}
