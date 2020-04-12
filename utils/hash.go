package utils

import (
	"crypto/md5"
)

const (
	// HashSize represents the size of bytes the Hash function generate.
	// For sha1 function, it will generates 20 bytes.
	HashSize = 16
)

func Hash(data []byte) []byte {
	res := make([]byte, HashSize)
	source := md5.Sum(data)
	copy(res, source[:HashSize-1])
	return res
}
