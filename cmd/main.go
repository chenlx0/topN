package main

import (
	"fmt"
	"log"
	"os"

	"github.com/chenlx0/topN/config"
	"github.com/chenlx0/topN/internal"
)

func main() {
	if len(os.Args) < 3 || os.Args[1] != "-c" {
		fmt.Println("use -c flag to specify config file")
		return
	}

	configPath := os.Args[2]
	conf, err := config.InitConfig(configPath)
	if err != nil {
		log.Fatal(err)
	}
	if err := internal.GenMiddleFiles(conf); err != nil {
		log.Fatal(err)
	}
	res, err := internal.Aggregate(conf)
	if err != nil {
		log.Fatal(err)
	}
	if err = internal.GenMsgData(conf.SourceFile, res); err != nil {
		log.Fatal(err)
	}
	for i := len(res) - 1; i >= 0; i-- {
		fmt.Printf("%d. string: %s occurs: %d\n", len(res)-i, res[i].GetDataStr(), res[i].GetOccurs())
	}
}
