package main

import (
	"testing"
	"time"

	"github.com/DreherTim/LOG8415E_FinalProject/internal/api"
)

func testCluster() *cluster {
	c := &cluster{primary: newNode("primary", "p", rolePrimary, nil)}
	for _, name := range []string{"replica1", "replica2"} {
		n := newNode(name, name, roleReplica, nil)
		n.healthy.Store(true)
		c.replicas = append(c.replicas, n)
	}
	c.primary.healthy.Store(true)
	return c
}

func TestWritesAlwaysGoToPrimary(t *testing.T) {
	c := testCluster()
	for _, p := range []api.Policy{api.PolicyDirect, api.PolicyRandom, api.PolicyLatency} {
		if n, _ := c.route(api.KindWrite, p); n != c.primary {
			t.Errorf("write with %s went to %s", p, n.name)
		}
	}
}

func TestDirectReadsGoToPrimary(t *testing.T) {
	c := testCluster()
	if n, _ := c.route(api.KindRead, api.PolicyDirect); n != c.primary {
		t.Errorf("direct read went to %s", n.name)
	}
}

func TestRandomSpreadsOverHealthyReplicasOnly(t *testing.T) {
	c := testCluster()
	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		n, _ := c.route(api.KindRead, api.PolicyRandom)
		counts[n.name]++
	}
	if counts["primary"] != 0 {
		t.Errorf("random read hit primary %d times", counts["primary"])
	}
	for _, r := range []string{"replica1", "replica2"} {
		if counts[r] < 4500 || counts[r] > 5500 { // ~uniform
			t.Errorf("%s got %d of 10000", r, counts[r])
		}
	}

	c.replicas[0].healthy.Store(false)
	for i := 0; i < 1000; i++ {
		if n, _ := c.route(api.KindRead, api.PolicyRandom); n.name != "replica2" {
			t.Fatalf("unhealthy replica selected: %s", n.name)
		}
	}
}

func TestLatencyPicksFastestHealthyReplica(t *testing.T) {
	c := testCluster()
	c.replicas[0].observeRTT(5 * time.Millisecond)
	c.replicas[1].observeRTT(1 * time.Millisecond)
	if n, _ := c.route(api.KindRead, api.PolicyLatency); n.name != "replica2" {
		t.Errorf("expected replica2, got %s", n.name)
	}
	c.replicas[1].healthy.Store(false)
	if n, _ := c.route(api.KindRead, api.PolicyLatency); n.name != "replica1" {
		t.Errorf("expected replica1 when replica2 unhealthy, got %s", n.name)
	}
}

func TestFallbackToPrimaryWhenNoReplicaHealthy(t *testing.T) {
	c := testCluster()
	for _, r := range c.replicas {
		r.healthy.Store(false)
	}
	for _, p := range []api.Policy{api.PolicyRandom, api.PolicyLatency} {
		n, fb := c.route(api.KindRead, p)
		if n != c.primary || !fb {
			t.Errorf("%s: got %s fallback=%v", p, n.name, fb)
		}
	}
}

func TestEWMASmoothsSpikes(t *testing.T) {
	n := newNode("r", "r", roleReplica, nil)
	for i := 0; i < 10; i++ {
		n.observeRTT(1 * time.Millisecond)
	}
	n.observeRTT(100 * time.Millisecond) // one spike
	if n.rtt() > 35*time.Millisecond {
		t.Errorf("single spike moved EWMA too much: %v", n.rtt())
	}
}
