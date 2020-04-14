package internal

import (
	"os"
)

// Msg represents each single string
type Msg struct {
	data   []byte
	hash   []byte
	offset int64
	occurs int
}

func (m *Msg) GetOccurs() int {
	return m.occurs
}

func (m *Msg) GetDataStr() string {
	return string(m.data)
}

// GenMsgData get line by offset, and sign to Msg
// but seems my implementation a little ugly
func GenMsgData(sourceFile string, msgList []*Msg) error {
	f, err := os.Open(sourceFile)
	if err != nil {
		return err
	}
	defer f.Close()

	tmpBytes := make([]byte, 16)
	var tmpResult []byte
	for _, v := range msgList {
		tmpResult = make([]byte, 0)
		f.Seek(v.offset, 0)
		for {
			if _, err = f.Read(tmpBytes); err != nil {
				return err
			}
			isOK := false
			for i := 0; i < len(tmpBytes); i++ {
				if tmpBytes[i] != '\n' {
					tmpResult = append(tmpResult, tmpBytes[i])
				} else {
					isOK = true
					break
				}
			}
			if isOK {
				break
			}
		}
		res := make([]byte, len(tmpResult))
		copy(res, tmpResult)
		v.data = res
	}
	return nil
}
