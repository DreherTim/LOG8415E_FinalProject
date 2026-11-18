package main

// lag.go — application-level replication-lag probe.
//
// Seconds_Behind_Source only has 1-second resolution and is 0 for any lag
// below a second, which is the normal case here. So the Proxy measures lag
// directly:
//
//  1. UPDATE proxy_meta.heartbeat SET seq = N on the primary and note the
//     time the commit returned (t0).
//  2. Poll each replica every few milliseconds until it shows seq >= N.
//  3. lag = time first seen on replica - t0.
//
// Both timestamps come from the Proxy's own clock, so clock skew between
// servers does not matter. Resolution is roughly the poll interval plus one
// replica round trip.

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	qHeartbeatWrite = `UPDATE proxy_meta.heartbeat SET seq = ?, written_at = NOW(6) WHERE id = 1`
	qHeartbeatRead  = `SELECT seq FROM proxy_meta.heartbeat WHERE id = 1`
	lagPollEvery    = 2 * time.Millisecond
)

func (c *cluster) probeLag(ctx context.Context, interval, maxWait time.Duration, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	var seq int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		seq++
		if err := c.probeOnce(ctx, seq, maxWait, log); err != nil {
			log.Warn("lag probe write failed", "err", err)
		}
	}
}

func (c *cluster) probeOnce(ctx context.Context, seq int64, maxWait time.Duration, log *slog.Logger) error {
	wctx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()
	st, err := c.primary.stmt(wctx, qHeartbeatWrite)
	if err != nil {
		return err
	}
	if _, err := st.ExecContext(wctx, seq); err != nil {
		return err
	}
	t0 := time.Now()

	var wg sync.WaitGroup
	for _, r := range c.replicas {
		if !r.healthy.Load() {
			continue
		}
		wg.Add(1)
		go func(r *node) {
			defer wg.Done()
			lag, ok := waitForSeq(ctx, r, seq, t0, maxWait)
			if !ok {
				r.lagNs.Store(-1)
				log.Warn("replication lag probe timed out", "node", r.name, "max_wait_ms", maxWait.Milliseconds())
				return
			}
			r.lagNs.Store(int64(lag))
			log.Info("replication_lag", "node", r.name,
				"lag_ms", float64(lag.Microseconds())/1000,
				"seconds_behind_source", r.secondsBehind.Load())
		}(r)
	}
	wg.Wait()
	return nil
}

func waitForSeq(ctx context.Context, r *node, seq int64, t0 time.Time, maxWait time.Duration) (time.Duration, bool) {
	ctx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()
	st, err := r.stmt(ctx, qHeartbeatRead)
	if err != nil {
		return 0, false
	}
	for {
		var got int64
		if err := st.QueryRowContext(ctx).Scan(&got); err == nil && got >= seq {
			return time.Since(t0), true
		}
		select {
		case <-ctx.Done():
			return 0, false
		case <-time.After(lagPollEvery):
		}
	}
}
