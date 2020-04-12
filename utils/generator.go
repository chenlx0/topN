package utils

import (
	"math/rand"
	"os"
	"time"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomURL() []byte {
	b := make([]byte, 1+rand.Int()%1024)
	for i := range b {
		b[i] = letters[rand.Int()%len(letters)]
	}
	return b
}

// GenSourceFile generates urls int specified size that splited by '\n'
func GenSourceFile(path string, size int64) error {
	rand.Seed(time.Now().UnixNano())
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer f.Close()

	totalSize := int64(0)
	var tmpBytes []byte
	for totalSize < size {
		tmpBytes = append(randomURL(), []byte("\n")...)
		n, err := f.Write(tmpBytes)
		if err != nil {
			return err
		}
		totalSize += int64(n)
	}
	return nil
}
