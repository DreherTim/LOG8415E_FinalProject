package main

// nodes.go — the MySQL nodes, their health/latency monitoring, and the
// three routing policies.

import (
	"context"
	"database/sql"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DreherTim/LOG8415E_FinalProject/internal/api"
)

type role string

const (
	rolePrimary role = "primary"
	roleReplica role = "replica"
)

// node is one MySQL server. All mutable fields are atomics because the
// monitor goroutines write them while request handlers read them.
type node struct {
	name string
	addr string
	role role
	db   *sql.DB

	healthy       atomic.Bool
	rttEWMA       atomic.Int64 // health-check round trip, smoothed (ns)
	secondsBehind atomic.Int64 // Seconds_Behind_Source, -1 = unknown
	lagNs         atomic.Int64 // last application-level lag probe (ns), -1 = unknown
	served        atomic.Int64 // requests routed here

	stmts sync.Map // query string -> *sql.Stmt (prepared once per node)
}

func newNode(name, addr string, r role, db *sql.DB) *node {
	n := &node{name: name, addr: addr, role: r, db: db}
	n.secondsBehind.Store(-1)
	n.lagNs.Store(-1)
	return n
}

func (n *node) rtt() time.Duration { return time.Duration(n.rttEWMA.Load()) }

// observeRTT folds a new sample into an exponentially weighted moving average
// (alpha = 0.3): reacts within a few samples but ignores single spikes.
func (n *node) observeRTT(d time.Duration) {
	const alpha = 0.3
	old := n.rttEWMA.Load()
	if old == 0 {
		n.rttEWMA.Store(int64(d))
		return
	}
	n.rttEWMA.Store(int64(alpha*float64(d) + (1-alpha)*float64(old)))
}

// stmt returns a prepared statement for query on this node, preparing it on
// first use. database/sql re-prepares transparently on new connections.
func (n *node) stmt(ctx context.Context, query string) (*sql.Stmt, error) {
	if s, ok := n.stmts.Load(query); ok {
		return s.(*sql.Stmt), nil
	}
	s, err := n.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	if existing, loaded := n.stmts.LoadOrStore(query, s); loaded {
		_ = s.Close()
		return existing.(*sql.Stmt), nil
	}
	return s, nil
}

// ---------- cluster & routing ----------

type cluster struct {
	primary  *node
	replicas []*node
}

func (c *cluster) all() []*node { return append([]*node{c.primary}, c.replicas...) }

func (c *cluster) healthyReplicas() []*node {
	out := make([]*node, 0, len(c.replicas))
	for _, r := range c.replicas {
		if r.healthy.Load() {
			out = append(out, r)
		}
	}
	return out
}

// route picks the node for one request.
//
//	writes          -> primary (always; strong consistency)
//	direct  reads   -> primary
//	random  reads   -> uniformly random healthy replica
//	latency reads   -> healthy replica with the lowest smoothed RTT
//
// If no replica is healthy, reads fall back to the primary (fallback=true).
func (c *cluster) route(kind api.OpKind, policy api.Policy) (n *node, fallback bool) {
	if kind == api.KindWrite || policy == api.PolicyDirect {
		return c.primary, false
	}
	healthy := c.healthyReplicas()
	if len(healthy) == 0 {
		return c.primary, true
	}
	switch policy {
	case api.PolicyRandom:
		return healthy[rand.IntN(len(healthy))], false
	case api.PolicyLatency:
		best := healthy[0]
		for _, r := range healthy[1:] {
			if r.rtt() < best.rtt() {
				best = r
			}
		}
		return best, false
	}
	return c.primary, true
}

// ---------- health monitoring ----------

// monitor checks every node every interval until ctx is cancelled.
func (c *cluster) monitor(ctx context.Context, interval time.Duration, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		var wg sync.WaitGroup
		for _, n := range c.all() {
			wg.Add(1)
			go func(n *node) { defer wg.Done(); n.check(ctx, interval, log) }(n)
		}
		wg.Wait()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// check measures RTT with "SELECT 1"; for replicas it also verifies that
// replication is running. A replica whose replication has stopped would
// serve ever-staler data, so it is taken out of rotation.
func (n *node) check(ctx context.Context, timeout time.Duration, log *slog.Logger) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	var one int
	err := n.db.QueryRowContext(cctx, "SELECT 1").Scan(&one)
	rtt := time.Since(start)

	ok := err == nil
	if ok {
		n.observeRTT(rtt)
		if n.role == roleReplica {
			running, sbs, rerr := replicaStatus(cctx, n.db)
			n.secondsBehind.Store(sbs)
			if rerr != nil || !running {
				ok, err = false, rerr
			}
		}
	}

	if was := n.healthy.Swap(ok); was != ok {
		log.Warn("node health changed", "node", n.name, "healthy", ok, "err", errString(err))
	}
}

// replicaStatus reads SHOW REPLICA STATUS. Its column set varies between
// MySQL versions, so it is scanned dynamically by column name.
func replicaStatus(ctx context.Context, db *sql.DB) (running bool, secondsBehind int64, err error) {
	rows, err := db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return false, -1, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return false, -1, err
	}
	if !rows.Next() {
		return false, -1, nil // replication not configured
	}
	vals := make([]sql.RawBytes, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return false, -1, err
	}
	m := make(map[string]string, len(cols))
	for i, c := range cols {
		m[c] = string(vals[i])
	}

	secondsBehind = -1
	if v, perr := strconv.ParseInt(m["Seconds_Behind_Source"], 10, 64); perr == nil {
		secondsBehind = v
	}
	running = m["Replica_IO_Running"] == "Yes" && m["Replica_SQL_Running"] == "Yes"
	return running, secondsBehind, nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
