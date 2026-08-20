package main

import (
	"os"

	harness "github.com/xxnuo/deepseek-harness-go"
)

func main() {
	os.Exit(harness.RunLandlockLauncher(os.Args[1:], os.Stdout, os.Stderr))
}
