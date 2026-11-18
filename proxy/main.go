// Proxy — routes validated requests to the MySQL cluster.
//
// It is the only component holding database credentials. It runs in the
// private subnet; its security group accepts traffic only from the Trusted
// Host's security group, and the MySQL nodes accept 3306 only from the
// Proxy's security group.
//
// Every request is logged with the node that served it (field "node").
//
// Configuration (environment variables):
//
//	LISTEN_ADDR          default ":8082"
//	PROXY_TOKEN          token the Trusted Host must present          (required)
//	DB_USER, DB_PASSWORD MySQL credentials                            (required)
//	DB_NAME              default "sakila"
//	PRIMARY_ADDR         e.g. "10.0.2.10:3306"                        (required)
//	REPLICA_ADDRS        e.g. "10.0.2.11:3306,10.0.2.12:3306"         (required)
//	                     optional names: "replica1=10.0.2.11:3306,..."
//	HEALTH_INTERVAL      default "500ms"
//	LAG_PROBE_INTERVAL   default "2s"  ("0" disables the probe)
//	QUERY_TIMEOUT        default "3s"
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"

	"github.com/DreherTim/LOG8415E_FinalProject/internal/api"
	"github.com/DreherTim/LOG8415E_FinalProject/internal/httpx"
)

const maxRequestBody = 8 << 10

type config struct {
	listenAddr       string
	proxyToken       []byte
	dbUser, dbPass   string
	dbName           string
	primaryAddr      string
	replicas         [][2]string // name, addr
	healthInterval   time.Duration
	lagProbeInterval time.Duration
	queryTimeout     time.Duration
}

func loadConfig() (config, error) {
	cfg := config{
		listenAddr:  httpx.Getenv("LISTEN_ADDR", ":8082"),
		proxyToken:  []byte(os.Getenv("PROXY_TOKEN")),
		dbUser:      os.Getenv("DB_USER"),
		dbPass:      os.Getenv("DB_PASSWORD"),
		dbName:      httpx.Getenv("DB_NAME", "sakila"),
		primaryAddr: os.Getenv("PRIMARY_ADDR"),
	}
	if len(cfg.proxyToken) == 0 || cfg.dbUser == "" || cfg.dbPass == "" || cfg.primaryAddr == "" {
		return cfg, errors.New("PROXY_TOKEN, DB_USER, DB_PASSWORD and PRIMARY_ADDR are required")
	}
	for i, item := range strings.Split(os.Getenv("REPLICA_ADDRS"), ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, addr, named := strings.Cut(item, "=")
		if !named {
			name, addr = fmt.Sprintf("replica%d", i+1), item
		}
		cfg.replicas = append(cfg.replicas, [2]string{name, addr})
	}
	if len(cfg.replicas) == 0 {
		return cfg, errors.New("REPLICA_ADDRS is required")
	}
	var err error
	if cfg.healthInterval, err = time.ParseDuration(httpx.Getenv("HEALTH_INTERVAL", "500ms")); err != nil {
		return cfg, fmt.Errorf("HEALTH_INTERVAL: %w", err)
	}
	if cfg.lagProbeInterval, err = time.ParseDuration(httpx.Getenv("LAG_PROBE_INTERVAL", "2s")); err != nil {
		return cfg, fmt.Errorf("LAG_PROBE_INTERVAL: %w", err)
	}
	if cfg.queryTimeout, err = time.ParseDuration(httpx.Getenv("QUERY_TIMEOUT", "3s")); err != nil {
		return cfg, fmt.Errorf("QUERY_TIMEOUT: %w", err)
	}
	return cfg, nil
}

// openDB creates a connection pool for one node. It does not connect yet;
// the health monitor discovers whether the node is reachable.
func openDB(cfg config, addr string) (*sql.DB, error) {
	mc := mysql.NewConfig()
	mc.User, mc.Passwd = cfg.dbUser, cfg.dbPass
	mc.Net, mc.Addr, mc.DBName = "tcp", addr, cfg.dbName
	mc.ParseTime = true
	mc.Timeout = 2 * time.Second
	mc.ReadTimeout = 5 * time.Second
	mc.WriteTimeout = 5 * time.Second

	connector, err := mysql.NewConnector(mc)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(64)
	db.SetConnMaxLifetime(5 * time.Minute)
	return db, nil
}

type proxy struct {
	cfg     config
	cluster *cluster
	log     *slog.Logger
}

func (p *proxy) handleExecute(c *gin.Context) {
	// 1. Strict decode + re-validation (never trust the previous hop).
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxRequestBody)
	var req api.ExecuteRequest
	dec := json.NewDecoder(c.Request.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		httpx.Reject(c, http.StatusBadRequest, "malformed JSON body")
		return
	}
	c.Set("op", req.Op)
	c.Set("policy", string(req.Policy))

	policy, ok := api.ParsePolicy(string(req.Policy))
	if !ok {
		httpx.Reject(c, http.StatusBadRequest, "invalid policy")
		return
	}
	kind, params, err := api.ParseOp(req.Op, req.Params)
	if err != nil {
		httpx.Reject(c, http.StatusBadRequest, err.Error())
		return
	}
	if kind != req.Kind {
		httpx.Reject(c, http.StatusBadRequest, "kind mismatch")
		return
	}

	// 2. Route.
	n, fallback := p.cluster.route(kind, policy)
	c.Set("node", n.name)
	n.served.Add(1)
	if fallback {
		p.log.Warn("no healthy replica, read sent to primary", "request_id", c.GetString("request_id"))
	}

	// 3. Execute.
	ctx, cancel := context.WithTimeout(c.Request.Context(), p.cfg.queryTimeout)
	defer cancel()
	start := time.Now()
	res, err := execute(ctx, n, params)
	dbMs := float64(time.Since(start).Microseconds()) / 1000
	if err != nil {
		p.log.Error("query failed", "request_id", c.GetString("request_id"), "node", n.name, "err", err)
		httpx.Reject(c, http.StatusBadGateway, "database error")
		return
	}

	c.JSON(http.StatusOK, api.ExecuteResponse{
		RequestID:    c.GetString("request_id"),
		Node:         n.name,
		Role:         string(n.role),
		Kind:         kind,
		Policy:       policy,
		Fallback:     fallback,
		DBMs:         dbMs,
		Rows:         res.rows,
		RowsAffected: res.rowsAffected,
		LastInsertID: res.lastInsertID,
	})
}

// handleStatus reports per-node health, latency, lag and request counts.
func (p *proxy) handleStatus(c *gin.Context) {
	type nodeStatus struct {
		Name                string   `json:"name"`
		Role                string   `json:"role"`
		Addr                string   `json:"addr"`
		Healthy             bool     `json:"healthy"`
		RTTMs               float64  `json:"rtt_ms"`
		SecondsBehindSource *int64   `json:"seconds_behind_source,omitempty"`
		LagMs               *float64 `json:"lag_ms,omitempty"`
		Served              int64    `json:"served"`
	}
	var out []nodeStatus
	for _, n := range p.cluster.all() {
		s := nodeStatus{
			Name: n.name, Role: string(n.role), Addr: n.addr,
			Healthy: n.healthy.Load(),
			RTTMs:   float64(n.rtt().Microseconds()) / 1000,
			Served:  n.served.Load(),
		}
		if v := n.secondsBehind.Load(); v >= 0 {
			s.SecondsBehindSource = &v
		}
		if v := n.lagNs.Load(); v >= 0 {
			ms := float64(v) / 1e6
			s.LagMs = &ms
		}
		out = append(out, s)
	}
	c.JSON(http.StatusOK, gin.H{"nodes": out})
}

func main() {
	log := httpx.NewLogger("proxy")

	cfg, err := loadConfig()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	// Build the cluster.
	pdb, err := openDB(cfg, cfg.primaryAddr)
	if err != nil {
		log.Error("primary", "err", err)
		os.Exit(1)
	}
	cl := &cluster{primary: newNode("primary", cfg.primaryAddr, rolePrimary, pdb)}
	for _, r := range cfg.replicas {
		db, err := openDB(cfg, r[1])
		if err != nil {
			log.Error("replica", "node", r[0], "err", err)
			os.Exit(1)
		}
		cl.replicas = append(cl.replicas, newNode(r[0], r[1], roleReplica, db))
	}

	// Background monitors.
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go cl.monitor(ctx, cfg.healthInterval, log)
	if cfg.lagProbeInterval > 0 {
		go cl.probeLag(ctx, cfg.lagProbeInterval, 2*time.Second, log)
	}

	p := &proxy{cfg: cfg, cluster: cl, log: log}

	r := gin.New()
	_ = r.SetTrustedProxies(nil)
	r.Use(gin.Recovery(), httpx.RequestID(true), httpx.AccessLog(log))

	auth := httpx.BearerAuth([][]byte{cfg.proxyToken})
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/v1/status", auth, p.handleStatus)
	r.POST("/v1/execute", auth, p.handleExecute)

	log.Info("config", "primary", cfg.primaryAddr, "replicas", cfg.replicas)
	httpx.Serve(log, cfg.listenAddr, r)
}
