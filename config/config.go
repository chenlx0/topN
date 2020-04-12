package config

import (
	"encoding/json"
	"io/ioutil"
)

// TopNConfig is the gobal config for topN programe
type TopNConfig struct {
	SourceFile  string `json:"sourceFile"`
	TmpFileDir  string `json:"tmpFileDir"`
	SplitNum    int    `json:"splitNum"`
	Concurrents int    `json:"concurrents"`
	N           int    `json:"N"`
}

// InitConfig parse and generate TopNConfig
func InitConfig(path string) (*TopNConfig, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	res := new(TopNConfig)
	if err = json.Unmarshal(data, res); err != nil {
		return nil, err
	}
	return res, nil
}
