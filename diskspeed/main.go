package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/rs/zerolog/log"
)

// checks disk speed by loading various size of data from disk to memory.
var storagePath = flag.String("path", "", "path of the data to be loaded")

const SaveDisk = false

func main() {
	// get input
	flag.Parse()

	if *storagePath == "" {
		fmt.Println("usage: -path [file path]")
		return
	}

	fileInfo, err := os.Stat(*storagePath)
	if err != nil {
		log.Error().Err(err).Msg("failed to get fileInfo")
	}

	t0 := time.Now()

	err = Read(*storagePath, int(fileInfo.Size()))
	if err != nil {
		log.Error().Err(err).Msg("failed to load")
	}

	t1 := time.Since(t0)
	throughput := float64(fileInfo.Size()) / t1.Seconds() / math.Pow(2, 20)

	fmt.Printf("File size: %v\n", t1)
	fmt.Printf("Time to load: %v\n", fileInfo.Size())
	fmt.Printf("Throughput: %.2f MiB/s\n", throughput)
}

func Read(fp string, size int) error {

	if fp == "" {
		return fmt.Errorf("no data source specified")
	}

	// the layer is in disk
	f, err := os.Open(fp)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, size)
	// loads file to memory
	_, err = f.ReadAt(buf, 0)
	if err != nil {
		return err
	}

	return nil
}
