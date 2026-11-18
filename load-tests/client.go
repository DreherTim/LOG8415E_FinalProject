package main

// client.go — sending requests and timing them.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// record is one measured request.
type record struct {
	Policy    string
	Phase     string // warmup | write | read | stale_write | stale_read
	Op        string
	Status    int
	LatencyMs float64
	Node      string // database that served it (from the Proxy's response)
	Err       string
}

// response holds the fields we need from the reply.
type response struct {
	Node         string           `json:"node"`
	Rows         []map[string]any `json:"rows"`
	LastInsertID int64            `json:"last_insert_id"`
	Error        string           `json:"error"`
}

type client struct {
	url, key string
	http     *http.Client
}

func newClient(url, key string) *client {
	return &client{url: url, key: key, http: &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: 100}, // reuse connections
	}}
}

// send performs one request and returns its record and parsed response.
func (c *client) send(policy, phase string, o op) (record, response) {
	rec := record{Policy: policy, Phase: phase, Op: o.Name}
	body, _ := json.Marshal(o)
	req, _ := http.NewRequest(http.MethodPost, c.url+"/v1/query", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+c.key)
	req.Header.Set("X-Policy", policy)

	start := time.Now()
	resp, err := c.http.Do(req)
	var r response
	if err == nil {
		err = json.NewDecoder(resp.Body).Decode(&r)
		resp.Body.Close()
		rec.Status = resp.StatusCode
	}
	rec.LatencyMs = float64(time.Since(start).Microseconds()) / 1000

	switch {
	case err != nil:
		rec.Err = err.Error()
	case rec.Status != http.StatusOK:
		rec.Err = r.Error
	default:
		rec.Node = r.Node
	}
	return rec, r
}

// run sends n requests (made by next) with `workers` in parallel and returns
// all records plus the total wall-clock time.
func (c *client) run(policy, phase string, n, workers int, next func() op) ([]record, time.Duration) {
	ops := make(chan op)
	var mu sync.Mutex
	var wg sync.WaitGroup
	records := make([]record, 0, n)

	start := time.Now()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for o := range ops {
				rec, _ := c.send(policy, phase, o)
				mu.Lock()
				records = append(records, rec)
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < n; i++ {
		ops <- next()
	}
	close(ops)
	wg.Wait()
	return records, time.Since(start)
}

// staleCheck inserts an actor and immediately reads it back with the same
// policy, n times. If the read hits a replica that has not applied the insert
// yet, the row is missing: a stale read. Runs one pair at a time.
func (c *client) staleCheck(policy string, n int) (*staleResult, []record) {
	res := &staleResult{StaleByNode: map[string]int{}}
	var records []record

	for i := 0; i < n; i++ {
		w, wr := c.send(policy, "stale_write", op{"insert_actor",
			map[string]any{"first_name": "STALE", "last_name": randomName()}})
		records = append(records, w)
		if w.Err != "" {
			continue
		}
		r, rr := c.send(policy, "stale_read", op{"get_actor",
			map[string]any{"actor_id": wr.LastInsertID}})
		records = append(records, r)
		if r.Err != "" {
			continue
		}
		res.Checked++
		if len(rr.Rows) == 0 {
			res.Stale++
			res.StaleByNode[r.Node]++
		}
	}
	if res.Checked > 0 {
		res.StaleRate = float64(res.Stale) / float64(res.Checked)
	}
	return res, records
}
