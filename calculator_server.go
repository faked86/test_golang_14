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
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	sumValue int64
	subValue int64
	// quantile store as uint64, don't forget to convert back
	p95C    uint64
	p99C    uint64
	p95Rust uint64
	p99Rust uint64
)

type Slot struct {
	requests  int64
	timestamp int64
}

type RpsBuffer struct {
	slots [60]Slot
}

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
	metricsChan := make(chan [2][]float64)

	go periodicPrinter(printerStop, *interval)
	go worker(workerChan, workerDone, metricsChan)
	go metricsAggregator(metricsChan)

	rpsBuf := &RpsBuffer{}

	mux := http.NewServeMux()
	mux.HandleFunc("/calc", makeCalcHandler(workerChan, rpsBuf))
	mux.HandleFunc("/metrics", makeMetricsHandler(rpsBuf))

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

func worker(requests <-chan int64, done chan<- struct{}, metricsChan chan<- [2][]float64) {
	defer close(done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	cDurations := make([]float64, 0, 50000)
	rustDurations := make([]float64, 0, 50000)

	for {
		select {
		case num, ok := <-requests:
			if !ok {
				if len(cDurations) > 0 || len(rustDurations) > 0 {
					metricsChan <- [2][]float64{cDurations, rustDurations}
				}
				return
			}

			cSum := C.int64_t(sumValue)
			cSub := C.int64_t(subValue)
			cNum := C.int64_t(num)

			startC := time.Now()
			atomic.StoreInt64(&sumValue, int64(C.add(cSum, cNum)))
			durationC := time.Since(startC).Seconds()
			cDurations = append(cDurations, durationC)

			startRust := time.Now()
			atomic.StoreInt64(&subValue, int64(C.sub(cSub, cNum)))
			durationRust := time.Since(startRust).Seconds()
			rustDurations = append(rustDurations, durationRust)

		case <-ticker.C:
			if len(cDurations) > 0 || len(rustDurations) > 0 {
				metricsChan <- [2][]float64{cDurations, rustDurations}
				cDurations = make([]float64, 0, 50000)
				rustDurations = make([]float64, 0, 50000)
			}
		}
	}
}

func metricsAggregator(metricsChan <-chan [2][]float64) {
	for metrics := range metricsChan {
		cDurations := metrics[0]
		rustDurations := metrics[1]

		if len(cDurations) > 0 {
			sort.Float64s(cDurations)
			cIdx95 := int(float64(len(cDurations)) * 0.95)
			cIdx99 := int(float64(len(cDurations)) * 0.99)
			atomic.StoreUint64(&p95C, math.Float64bits(cDurations[cIdx95]))
			atomic.StoreUint64(&p99C, math.Float64bits(cDurations[cIdx99]))
		} else {
			atomic.StoreUint64(&p95C, 0)
			atomic.StoreUint64(&p99C, 0)
		}

		if len(rustDurations) > 0 {
			sort.Float64s(rustDurations)
			rustIdx95 := int(float64(len(rustDurations)) * 0.95)
			rustIdx99 := int(float64(len(rustDurations)) * 0.99)
			atomic.StoreUint64(&p95Rust, math.Float64bits(rustDurations[rustIdx95]))
			atomic.StoreUint64(&p99Rust, math.Float64bits(rustDurations[rustIdx99]))
		} else {
			atomic.StoreUint64(&p95Rust, 0)
			atomic.StoreUint64(&p99Rust, 0)
		}
	}
}

func makeCalcHandler(workerChan chan<- int64, rpsBuf *RpsBuffer) http.HandlerFunc {
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

		now := time.Now().Unix()
		index := now % 60
		for {
			timestamp := atomic.LoadInt64(&rpsBuf.slots[index].timestamp)
			if timestamp == now {
				atomic.AddInt64(&rpsBuf.slots[index].requests, 1)
				break
			}
			if atomic.CompareAndSwapInt64(&rpsBuf.slots[index].timestamp, timestamp, now) {
				atomic.StoreInt64(&rpsBuf.slots[index].requests, 1)
				break
			}
		}

		select {
		case workerChan <- int64(num):
			w.Write([]byte("ok"))
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("queue full"))
		}
	}
}

func makeMetricsHandler(rpsBuf *RpsBuffer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}

		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			w.Write([]byte("method NOT allowed"))
			return
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		responseBody := make([]byte, 0)
		responseBody = fmt.Appendf(responseBody, "# HELP http_requests_per_second HTTP requests per second for the last 60 seconds\n")
		responseBody = fmt.Appendf(responseBody, "# TYPE http_requests_per_second gauge\n")

		now := time.Now().Unix()
		for i := 59; i >= 0; i-- {
			targetTime := now - int64(i)
			index := targetTime % 60
			rps := atomic.LoadInt64(&rpsBuf.slots[index].requests)
			timestamp := atomic.LoadInt64(&rpsBuf.slots[index].timestamp)
			if timestamp != targetTime {
				rps = 0
			}
			responseBody = fmt.Appendf(responseBody, "http_requests_per_second{offset_seconds=\"%d\"} %d\n", i, rps)
		}

		responseBody = fmt.Append(responseBody, "# HELP c_function_duration_seconds Execution time of the C function in seconds\n")
		responseBody = fmt.Append(responseBody, "# TYPE c_function_duration_seconds summary\n")

		responseBody = fmt.Appendf(responseBody, "c_function_duration_seconds{quantile=\"0.95\"} %f\n", math.Float64frombits(atomic.LoadUint64(&p95C)))
		responseBody = fmt.Appendf(responseBody, "c_function_duration_seconds{quantile=\"0.99\"} %f\n", math.Float64frombits(atomic.LoadUint64(&p99C)))

		responseBody = fmt.Append(responseBody, "# HELP rust_function_duration_seconds Execution time of the Rust function in seconds\n")
		responseBody = fmt.Append(responseBody, "# TYPE rust_function_duration_seconds summary\n")

		responseBody = fmt.Appendf(responseBody, "rust_function_duration_seconds{quantile=\"0.95\"} %f\n", math.Float64frombits(atomic.LoadUint64(&p95Rust)))
		responseBody = fmt.Appendf(responseBody, "rust_function_duration_seconds{quantile=\"0.99\"} %f\n", math.Float64frombits(atomic.LoadUint64(&p99Rust)))

		w.Write(responseBody)
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
