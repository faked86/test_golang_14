package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

type Stats struct {
	ok  int64
	err int64
}

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

	clientTimeout := time.Duration(*timeout * float64(time.Second))

	customTransport := &http.Transport{
		MaxIdleConns:        *threads,
		MaxIdleConnsPerHost: *threads,
		IdleConnTimeout:     90 * time.Second,
	}

	client := &http.Client{
		Transport: customTransport,
		Timeout:   clientTimeout,
	}

	done := make(chan struct{})
	stats := &Stats{
		ok:  0,
		err: 0,
	}

	for i := 0; i < *threads; i++ {
		go worker(client, i, *url, done, *interval, stats)
	}

	fmt.Printf("Generator started: %d threads -> %s\n", *threads, *url)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop
	fmt.Printf("\nSIGINT received, shutting down...\n")
	close(done)

	fmt.Printf("Total requests: ok=%d errors=%d", atomic.LoadInt64(&stats.ok), atomic.LoadInt64(&stats.err))
}

func worker(client *http.Client, id int, baseURL string, done <-chan struct{}, interval float64, stats *Stats) {
	for {
		select {
		case <-done:
			return
		default:
			if interval > 0 {
				time.Sleep(time.Duration(interval * float64(time.Second)))
			}

			num := rand.N(int64(201)) - 100

			urlStr := fmt.Sprintf("%s?num=%d", baseURL, num)

			resp, err := client.Post(urlStr, "application/x-www-form-urlencoded", nil)
			if err != nil {
				atomic.AddInt64(&stats.err, 1)
				fmt.Printf("[worker %d] request failed: %v\n", id, err)
				continue
			}
			if resp.StatusCode != http.StatusOK {
				atomic.AddInt64(&stats.err, 1)

				bodyBytes, _ := io.ReadAll(resp.Body)
				fmt.Printf("[worker %d] request failed with status %d: %s\n", id, resp.StatusCode, string(bodyBytes))

				resp.Body.Close()
				continue
			}

			atomic.AddInt64(&stats.ok, 1)

			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}
}
