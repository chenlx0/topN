package utils

import "testing"

func TestGenerator(t *testing.T) {
	GenSourceFile("/tmp/test.txt", 5*1024*1024*1024)
}
