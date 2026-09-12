package main

/*
#cgo LDFLAGS: -L. -lcalculator -lcalculator_rust
#include <stdint.h>

int64_t add(int64_t a, int64_t b);
int64_t sub(int64_t a, int64_t b);
*/
import "C"

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	sumValue int64
	subValue int64
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Calculator HTTP server\n\n")
		flag.PrintDefaults()
	}

	host := flag.String("host", "0.0.0.0", "")
	port := flag.Int("port", 8080, "")
	interval := flag.Float64("interval", 5.0, "seconds between periodic sum/sub reports")

	flag.Parse()

	printerStop := make(chan struct{})
	workerChan := make(chan int64, 10000)
	workerDone := make(chan struct{})

	go periodicPrinter(printerStop, *interval)
	go worker(workerChan, workerDone)

	mux := http.NewServeMux()
	mux.HandleFunc("/calc", makeCalcHandler(workerChan))

	serverAddr := fmt.Sprintf("%s:%d", *host, *port)
	server := &http.Server{
		Addr:    serverAddr,
		Handler: mux,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server ListenAndServe: %v", err)
		}
	}()

	<-stop
	fmt.Printf("\nSIGINT received, shutting down...\n")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf("HTTP server Shutdown: %v\n", err)
	}

	close(printerStop)
	close(workerChan)
	<-workerDone

	printTotals("final")
}

func worker(requests <-chan int64, done chan<- struct{}) {
	defer close(done)
	for num := range requests {
		cSum := C.int64_t(sumValue)
		cSub := C.int64_t(subValue)
		cNum := C.int64_t(num)

		// startC := time.Now()
		atomic.StoreInt64(&sumValue, int64(C.add(cSum, cNum)))
		// durationC := time.Since(startC)

		// startRust := time.Now()
		atomic.StoreInt64(&subValue, int64(C.sub(cSub, cNum)))
		// durationRust := time.Since(startRust)
	}
}

func makeCalcHandler(workerChan chan<- int64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/calc" {
			http.NotFound(w, r)
			return
		}

		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			w.Write([]byte("method NOT allowed"))
			return
		}

		q := r.URL.Query()
		numRaw := q.Get("num")
		if numRaw == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing 'num' query parameter"))
			return
		}

		num, err := strconv.Atoi(numRaw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("'num' must be an integer"))
			return
		}

		select {
		case workerChan <- int64(num):
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("queue full"))
		}
	}
}

func printTotals(label string) {
	sumV := atomic.LoadInt64(&sumValue)
	subV := atomic.LoadInt64(&subValue)
	fmt.Printf("[%s] sum=%d sub=%d\n", label, sumV, subV)
}

func periodicPrinter(stop <-chan struct{}, intervalSeconds float64) {
	ticker := time.NewTicker(time.Duration(intervalSeconds * float64(time.Second)))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			printTotals("periodic")
		case <-stop:
			return
		}
	}
}
