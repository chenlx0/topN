package internal

import (
	"testing"

	"github.com/chenlx0/topN/config"
)

func TestGenSplitFiles(t *testing.T) {
	conf, err := config.InitConfig("../conf.json")
	if err != nil {
		t.Errorf("parse config failed: %v", err)
	}
	err = GenMiddleFiles(conf)
	if err != nil {
		t.Errorf("GenMiddleFiles failed: %v", err)
	}
}
