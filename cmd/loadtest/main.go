package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"

	"google.golang.org/protobuf/proto"
)

const baseURL = "http://localhost:4318"

type signalStats struct {
	name    string
	reqs    atomic.Int64
	errors  atomic.Int64
	latency atomic.Int64
	bytes   atomic.Int64
}

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║    STRESS TEST — Target 10K RPS                         ║")
	fmt.Println("╚══════════════════════════════════════════════════════════╝")
	fmt.Println()

	tracePayload := buildTracePayload(50)
	metricPayload := buildMetricPayload(20)
	logPayload := buildLogPayload(30)

	fmt.Printf("Payloads: traces=%d bytes  metrics=%d bytes  logs=%d bytes\n\n",
		len(tracePayload), len(metricPayload), len(logPayload))

	targets := []struct {
		url     string
		payload []byte
		stats   *signalStats
	}{
		{baseURL + "/v1/traces", tracePayload, &signalStats{name: "traces"}},
		{baseURL + "/v1/metrics", metricPayload, &signalStats{name: "metrics"}},
		{baseURL + "/v1/logs", logPayload, &signalStats{name: "logs"}},
	}

	// escalating concurrency
	levels := []int{200, 400, 800, 1200, 1600, 2000, 3000, 4000}
	phaseDuration := 20 * time.Second

	totalStart := time.Now()

	for _, concurrency := range levels {
		perSignal := int(math.Max(1, float64(concurrency/3)))

		fmt.Printf("━━━ Concurrency: %d (%d per signal) | Duration: %s ━━━\n",
			concurrency, perSignal, phaseDuration)

		// check if server is alive before starting
		if !isAlive() {
			fmt.Println("  !! SERVER IS DOWN — OOM KILLED !!")
			break
		}

		client := &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        concurrency * 2,
				MaxIdleConnsPerHost: concurrency,
				MaxConnsPerHost:     concurrency,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
				ForceAttemptHTTP2:   false,
				WriteBufferSize:     32768,
				ReadBufferSize:      4096,
			},
		}

		deadline := time.Now().Add(phaseDuration)
		phaseStart := time.Now()

		// snapshot before
		snapReqs := [3]int64{}
		snapErrs := [3]int64{}
		for i, t := range targets {
			snapReqs[i] = t.stats.reqs.Load()
			snapErrs[i] = t.stats.errors.Load()
		}

		var wg sync.WaitGroup
		oomDetected := atomic.Bool{}

		// launch workers
		for _, t := range targets {
			for w := 0; w < perSignal; w++ {
				wg.Add(1)
				go func(url string, payload []byte, stats *signalStats) {
					defer wg.Done()
					consecutiveFails := 0
					for time.Now().Before(deadline) && !oomDetected.Load() {
						start := time.Now()
						resp, err := client.Post(url, "application/x-protobuf", bytes.NewReader(payload))
						elapsed := time.Since(start)
						stats.latency.Add(elapsed.Microseconds())
						stats.bytes.Add(int64(len(payload)))

						if err != nil {
							stats.errors.Add(1)
							// need 10 consecutive fast failures to confirm OOM
							if elapsed < 100*time.Millisecond {
								consecutiveFails++
								if consecutiveFails >= 10 {
									oomDetected.Store(true)
								}
							}
							continue
						}
						consecutiveFails = 0
						resp.Body.Close()
						if resp.StatusCode != 200 {
							stats.errors.Add(1)
						}
						stats.reqs.Add(1)
					}
				}(t.url, t.payload, t.stats)
			}
		}

		// progress
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			var last [3]int64
			for i, t := range targets {
				last[i] = t.stats.reqs.Load()
			}
			for range ticker.C {
				if time.Now().After(deadline) || oomDetected.Load() {
					return
				}
				elapsed := time.Since(phaseStart).Seconds()
				fmt.Printf("  [%4.0fs]", elapsed)
				for i, t := range targets {
					cur := t.stats.reqs.Load()
					rps := float64(cur-last[i]) / 5.0
					fmt.Printf("  %s: %5.0f rps", t.stats.name, rps)
					last[i] = cur
				}

				// try to get memory stats
				memInfo := getCollectorStats()
				if memInfo != "" {
					fmt.Printf("  | %s", memInfo)
				}
				fmt.Println()
			}
		}()

		wg.Wait()
		phaseElapsed := time.Since(phaseStart)

		if oomDetected.Load() || !isAlive() {
			fmt.Println()
			fmt.Printf("  !! OOM DETECTED at concurrency=%d !!\n", concurrency)
			fmt.Println()
			break
		}

		// phase summary
		fmt.Printf("  Result:")
		for i, t := range targets {
			phaseReqs := t.stats.reqs.Load() - snapReqs[i]
			phaseErrs := t.stats.errors.Load() - snapErrs[i]
			rps := float64(phaseReqs) / phaseElapsed.Seconds()
			fmt.Printf("  %s=%d (%.0f/s, %d err)", t.stats.name, phaseReqs, rps, phaseErrs)
		}
		fmt.Println()

		// brief pause between phases
		time.Sleep(2 * time.Second)

		// memory check
		memInfo := getCollectorStats()
		if memInfo != "" {
			fmt.Printf("  Memory after phase: %s\n", memInfo)
		}
		fmt.Println()

		client.CloseIdleConnections()
	}

	totalElapsed := time.Since(totalStart)

	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║                 FINAL RESULTS                           ║")
	fmt.Println("╚══════════════════════════════════════════════════════════╝")
	fmt.Printf("Total duration: %s\n\n", totalElapsed.Round(time.Millisecond))

	var grandTotal, grandErrors, grandBytes int64
	for _, t := range targets {
		reqs := t.stats.reqs.Load()
		errs := t.stats.errors.Load()
		avg := int64(0)
		if reqs > 0 {
			avg = t.stats.latency.Load() / reqs
		}
		grandTotal += reqs
		grandErrors += errs
		grandBytes += t.stats.bytes.Load()
		fmt.Printf("%-10s  requests=%-8d  errors=%-6d  avg_latency=%.1fms\n",
			t.stats.name, reqs, errs, float64(avg)/1000)
	}

	fmt.Println()
	fmt.Printf("Grand total:  %d requests, %d errors\n", grandTotal, grandErrors)
	fmt.Printf("Overall RPS:  %.0f\n", float64(grandTotal)/totalElapsed.Seconds())
	fmt.Printf("Throughput:   %.1f MB/s\n", float64(grandBytes)/totalElapsed.Seconds()/1024/1024)

	if !isAlive() {
		fmt.Println("\n** Collector is DOWN — confirmed OOM kill **")
	} else {
		fmt.Println("\n** Collector survived all phases **")
	}
}

func isAlive() bool {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(baseURL + "/stats")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}

func getCollectorStats() string {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(baseURL + "/stats")
	if err != nil {
		return "unreachable"
	}
	defer resp.Body.Close()

	var buf [4096]byte
	n, _ := resp.Body.Read(buf[:])
	body := string(buf[:n])

	// quick parse for rss_mb, heap_alloc_mb, goroutines
	rss := extractJSON(body, "rss_mb")
	heap := extractJSON(body, "heap_alloc_mb")
	goroutines := extractJSON(body, "goroutines")

	return fmt.Sprintf("rss=%sMB heap=%sMB goroutines=%s", rss, heap, goroutines)
}

func extractJSON(body, key string) string {
	search := fmt.Sprintf(`"%s":`, key)
	idx := 0
	for i := 0; i < len(body)-len(search); i++ {
		if body[i:i+len(search)] == search {
			idx = i + len(search)
			break
		}
	}
	if idx == 0 {
		return "?"
	}
	end := idx
	for end < len(body) && body[end] != ',' && body[end] != '}' {
		end++
	}
	return body[idx:end]
}

// ─── Payload builders ───

func buildTracePayload(numSpans int) []byte {
	spans := make([]*tracepb.Span, numSpans)
	now := uint64(time.Now().UnixNano())
	for i := 0; i < numSpans; i++ {
		traceID := make([]byte, 16)
		spanID := make([]byte, 8)
		rand.Read(traceID)
		rand.Read(spanID)
		spans[i] = &tracepb.Span{
			TraceId:           traceID,
			SpanId:            spanID,
			Name:              fmt.Sprintf("oom-test-span-%d", i),
			Kind:              tracepb.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: now - uint64(100*time.Millisecond),
			EndTimeUnixNano:   now,
			Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
			Attributes: []*commonpb.KeyValue{
				strAttr("http.method", "POST"),
				strAttr("http.url", "/api/heavy-endpoint"),
				intAttr("http.status_code", 200),
				strAttr("http.user_agent", "loadtest/1.0 (OOM stress test)"),
				strAttr("net.peer.ip", "10.0.0.1"),
			},
		}
	}
	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: svcResource("oom-test-service"),
			ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
		}},
	}
	return mustMarshal(req)
}

func buildMetricPayload(numMetrics int) []byte {
	now := uint64(time.Now().UnixNano())
	metrics := make([]*metricspb.Metric, numMetrics)
	for i := 0; i < numMetrics; i++ {
		metrics[i] = &metricspb.Metric{
			Name: fmt.Sprintf("oom_metric_%d", i),
			Data: &metricspb.Metric_Gauge{
				Gauge: &metricspb.Gauge{
					DataPoints: []*metricspb.NumberDataPoint{{
						TimeUnixNano: now,
						Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: float64(i) * 3.14},
						Attributes: []*commonpb.KeyValue{
							strAttr("host", "oom-host"),
							strAttr("region", "stress-test"),
						},
					}},
				},
			},
		}
	}
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource:     svcResource("oom-test-metrics"),
			ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: metrics}},
		}},
	}
	return mustMarshal(req)
}

func buildLogPayload(numLogs int) []byte {
	now := uint64(time.Now().UnixNano())
	records := make([]*logspb.LogRecord, numLogs)
	for i := 0; i < numLogs; i++ {
		tid := make([]byte, 16)
		sid := make([]byte, 8)
		rand.Read(tid)
		rand.Read(sid)
		records[i] = &logspb.LogRecord{
			TimeUnixNano:   now,
			SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_WARN,
			SeverityText:   "WARN",
			Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{
				StringValue: fmt.Sprintf("OOM stress log %d trace=%s with extra padding to increase payload size for memory pressure testing", i, hex.EncodeToString(tid[:8])),
			}},
			TraceId: tid,
			SpanId:  sid,
			Attributes: []*commonpb.KeyValue{
				strAttr("component", "oom-test"),
				intAttr("log.index", int64(i)),
				strAttr("environment", "stress-test"),
			},
		}
	}
	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource:  svcResource("oom-test-logs"),
			ScopeLogs: []*logspb.ScopeLogs{{LogRecords: records}},
		}},
	}
	return mustMarshal(req)
}

func svcResource(name string) *resourcepb.Resource {
	return &resourcepb.Resource{
		Attributes: []*commonpb.KeyValue{strAttr("service.name", name)},
	}
}

func strAttr(key, val string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: val}},
	}
}

func intAttr(key string, val int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: val}},
	}
}

func mustMarshal(msg proto.Message) []byte {
	data, err := proto.Marshal(msg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal failed: %v\n", err)
		os.Exit(1)
	}
	return data
}
