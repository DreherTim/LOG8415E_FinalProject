package main

// stats.go — turning records into numbers, and saving them.

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type phaseStats struct {
	Requests      int            `json:"requests"`
	Errors        int            `json:"errors"`
	ErrorRate     float64        `json:"error_rate"`
	ThroughputRPS float64        `json:"throughput_rps"`
	MeanMs        float64        `json:"mean_ms"`
	P50Ms         float64        `json:"p50_ms"`
	P95Ms         float64        `json:"p95_ms"`
	P99Ms         float64        `json:"p99_ms"`
	Nodes         map[string]int `json:"nodes"` // successful requests per database
}

type staleResult struct {
	Checked     int            `json:"checked"`
	Stale       int            `json:"stale"`
	StaleRate   float64        `json:"stale_rate"`
	StaleByNode map[string]int `json:"stale_by_node"`
}

type policyResult struct {
	Write         phaseStats   `json:"write"`
	Read          phaseStats   `json:"read"`
	ReadYourWrite *staleResult `json:"read_your_write"`
}

func summarize(records []record, elapsed time.Duration) phaseStats {
	s := phaseStats{Requests: len(records), Nodes: map[string]int{}}
	var latencies []float64
	for _, r := range records {
		if r.Err != "" {
			s.Errors++
			continue
		}
		s.Nodes[r.Node]++
		latencies = append(latencies, r.LatencyMs)
	}
	if len(records) > 0 {
		s.ErrorRate = float64(s.Errors) / float64(len(records))
	}
	if len(latencies) == 0 {
		return s
	}

	sort.Float64s(latencies)
	sum := 0.0
	for _, l := range latencies {
		sum += l
	}
	s.ThroughputRPS = round(float64(len(latencies)) / elapsed.Seconds())
	s.MeanMs = round(sum / float64(len(latencies)))
	s.P50Ms = percentile(latencies, 50)
	s.P95Ms = percentile(latencies, 95)
	s.P99Ms = percentile(latencies, 99)
	return s
}

// percentile uses the nearest-rank method on sorted data.
func percentile(sorted []float64, p float64) float64 {
	i := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	return sorted[max(i, 0)]
}

func round(v float64) float64 { return math.Round(v*100) / 100 }

func (r policyResult) print() {
	for _, p := range []struct {
		name string
		s    phaseStats
	}{{"write", r.Write}, {"read", r.Read}} {
		fmt.Printf("  %-5s %6.1f req/s  p50 %6.2f  p95 %6.2f  p99 %6.2f ms  errors %d  nodes %v\n",
			p.name, p.s.ThroughputRPS, p.s.P50Ms, p.s.P95Ms, p.s.P99Ms, p.s.Errors, p.s.Nodes)
	}
	st := r.ReadYourWrite
	fmt.Printf("  stale reads: %d of %d (%.1f%%) %v\n\n", st.Stale, st.Checked, st.StaleRate*100, st.StaleByNode)
}

// save writes summary.json and requests.csv into dir.
func save(dir string, summary map[string]policyResult, records []record) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	js, _ := json.MarshalIndent(summary, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "summary.json"), js, 0o644); err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(dir, "requests.csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{"policy", "phase", "op", "status", "latency_ms", "node", "error"})
	for _, r := range records {
		w.Write([]string{r.Policy, r.Phase, r.Op, strconv.Itoa(r.Status),
			strconv.FormatFloat(r.LatencyMs, 'f', 3, 64), r.Node, r.Err})
	}
	w.Flush()
	return w.Error()
}
