package internal

import (
	"testing"

	"github.com/chenlx0/topN/config"
	"github.com/chenlx0/topN/utils"
)

func TestInternal(t *testing.T) {
	conf, err := config.InitConfig("../conf.json")
	if err != nil {
		t.Errorf("parse config failed: %v", err)
	}
	utils.GenSourceFile(conf.SourceFile, 128*1024*1024)
	err = GenMiddleFiles(conf)
	if err != nil {
		t.Errorf("GenMiddleFiles failed: %v", err)
	}
	msgList, _ := Aggregate(conf)
	err = GenMsgData(conf.SourceFile, msgList)
	if err != nil {
		t.Errorf("gen msg failed: %v", err)
	}
}
