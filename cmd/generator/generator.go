package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Load generator for the calculator\n\n")
		flag.PrintDefaults()
	}

	url := flag.String("url", "http://localhost:8080/calc", "calculator endpoint")

	threads := flag.Int("threads", 10, "number of worker threads")
	flag.IntVar(threads, "n", 10, "number of worker threads (shorthand)")

	interval := flag.Float64("interval", 0.1, "pause between requests per thread, in seconds (0 = as fast as possible)")

	timeout := flag.Float64("timeout", 5.0, "HTTP request timeout, seconds")

	flag.Parse()

	fmt.Printf("Generator started: %d threads -> %s\n", *threads, *url) //
}
