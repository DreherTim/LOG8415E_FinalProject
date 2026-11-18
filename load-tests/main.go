// Load generator: benchmarks the Gatekeeper -> Trusted Host -> Proxy chain.
//
// For each policy: warm-up, N writes, N reads, then read-your-write checks.
// Results go to results/<timestamp>/summary.json and requests.csv.
//
//	go run ./load-tests                                  # full run, all policies
//	go run ./load-tests -policies random -n 100          # quick test
//	go run ./load-tests -url http://<gatekeeper-ip>:8080 # against AWS
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	url := flag.String("url", "http://localhost:8080", "Gatekeeper base URL")
	key := flag.String("key", os.Getenv("CLIENT_API_KEYS"), "client API key")
	policies := flag.String("policies", "direct,random,latency", "policies to benchmark")
	n := flag.Int("n", 1000, "writes and reads per policy (each)")
	warmup := flag.Int("warmup", 100, "unmeasured warm-up requests per policy")
	staleChecks := flag.Int("stale-checks", 200, "read-your-write checks per policy")
	concurrency := flag.Int("concurrency", 16, "parallel requests")
	flag.Parse()

	if *key == "" {
		fmt.Println("missing API key: use -key or set CLIENT_API_KEYS")
		os.Exit(1)
	}
	c := newClient(*url, *key)

	var allRecords []record
	summary := map[string]policyResult{}

	for _, policy := range strings.Split(*policies, ",") {
		fmt.Println("==", policy)

		c.run(policy, "warmup", *warmup, *concurrency, randomOp) // not measured

		writes, writeTime := c.run(policy, "write", *n, *concurrency, randomWrite)
		reads, readTime := c.run(policy, "read", *n, *concurrency, randomRead)
		stale, staleRecords := c.staleCheck(policy, *staleChecks)

		res := policyResult{
			Write:         summarize(writes, writeTime),
			Read:          summarize(reads, readTime),
			ReadYourWrite: stale,
		}
		summary[policy] = res
		res.print()

		allRecords = append(allRecords, writes...)
		allRecords = append(allRecords, reads...)
		allRecords = append(allRecords, staleRecords...)
	}

	dir := filepath.Join("results", time.Now().Format("20060102-150405"))
	if err := save(dir, summary, allRecords); err != nil {
		fmt.Println("saving results:", err)
		os.Exit(1)
	}
	fmt.Println("results written to", dir)
}
