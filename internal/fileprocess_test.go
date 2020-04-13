package internal

import (
	"fmt"
	"testing"

	"github.com/chenlx0/topN/config"
)

func TestGenMiddleFiles(t *testing.T) {
	conf, err := config.InitConfig("../conf.json")
	if err != nil {
		t.Errorf("parse config failed: %v", err)
	}
	err = GenMiddleFiles(conf)
	if err != nil {
		t.Errorf("GenMiddleFiles failed: %v", err)
	}
}

func TestInternal(t *testing.T) {
	conf, err := config.InitConfig("../conf.json")
	if err != nil {
		t.Errorf("parse config failed: %v", err)
	}
	msgList, _ := Aggregate(conf)
	err = GenMsgData(conf.SourceFile, msgList)
	if err != nil {
		t.Errorf("gen msg failed: %v", err)
	}
	for _, v := range msgList {
		fmt.Printf("string: %s , occurs: %d\n", string(v.data), v.occurs)
	}
}
