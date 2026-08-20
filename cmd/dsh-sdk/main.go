package main

import (
	"context"
	"fmt"
	"os"

	harness "github.com/xxnuo/deepseek-harness-go"
)

func main() {
	e, err := harness.New()
	if err == nil {
		err = e.ServeJSONRPC(context.Background(), os.Stdin, os.Stdout)
	}
	if e != nil {
		_ = e.Close()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
