// Trusted Host which is the private service between the Gatekeeper and the Proxy
//
// runs in a private subnet with no public IP
// its security group accepts traffic only from the Gatekeeper's security group.
//
// Configuration (environment varialbes):
//
//	LISTEN_ADDR        default ":8081"
//	GATEKEEPER_TOKEN   token the Gatekeeper must present
//	PROXY_URL          e.g. "http://10.0.2.20:8082"
//	PROXY_TOKEN        shared secret Trusted Host -> Proxy
//	UPSTREAM_TIMEOUT   default "5s"
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/DreherTim/LOG8415E_FinalProject/internal/api"
	"github.com/DreherTim/LOG8415E_FinalProject/internal/httpx"
)

const maxRequestBody = 8 << 10

type config struct {
	listenAddr      string
	gatekeeperToken []byte
	proxyURL        string
	proxyToken      string
	upstreamTimeout time.Duration
}

func loadConfig() (config, error) {
	cfg := config{
		listenAddr:      httpx.Getenv("LISTEN_ADDR", ":8081"),
		gatekeeperToken: []byte(os.Getenv("GATEKEEPER_TOKEN")),
		proxyURL:        strings.TrimRight(os.Getenv("PROXY_URL"), "/"),
		proxyToken:      os.Getenv("PROXY_TOKEN"),
	}
	if len(cfg.gatekeeperToken) == 0 || cfg.proxyURL == "" || cfg.proxyToken == "" {
		return cfg, errors.New("GATEKEEPER_TOKEN, PROXY_URL and PROXY_TOKEN are required")
	}
	d, err := time.ParseDuration(httpx.Getenv("UPSTREAM_TIMEOUT", "5s"))
	if err != nil {
		return cfg, errors.New("UPSTREAM_TIMEOUT: " + err.Error())
	}
	cfg.upstreamTimeout = d
	return cfg, nil
}

type trustedHost struct {
	cfg    config
	client *http.Client
	log    *slog.Logger
}

func (t *trustedHost) handleExecute(c *gin.Context) {
	// 1. Strict decoding of the internal message
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

	// 2. Then re-validating everything
	if _, ok := api.ParsePolicy(string(req.Policy)); !ok || req.Policy == "" {
		httpx.Reject(c, http.StatusBadRequest, "invalid policy")
		return
	}
	kind, params, err := api.ParseOp(req.Op, req.Params)
	if err != nil {
		httpx.Reject(c, http.StatusBadRequest, err.Error())
		return
	}
	if kind != req.Kind {
		// A correct gatekeeper never sends this -> treat as suspicious.
		t.log.Warn("kind mismatch: possible compromised or buggy gatekeeper",
			"request_id", c.GetString("request_id"), "op", req.Op,
			"declared", req.Kind, "actual", kind)
		httpx.Reject(c, http.StatusBadRequest, "kind mismatch")
		return
	}

	// 3. Forward the request to proxy
	canonical, _ := json.Marshal(params)
	body, _ := json.Marshal(api.ExecuteRequest{
		RequestID: c.GetString("request_id"),
		Op:        req.Op,
		Kind:      kind,
		Policy:    req.Policy,
		Params:    canonical,
	})

	ctx, cancel := context.WithTimeout(c.Request.Context(), t.cfg.upstreamTimeout)
	defer cancel()
	status, respBody, err := httpx.Forward(ctx, t.client,
		t.cfg.proxyURL+"/v1/execute", t.cfg.proxyToken, c.GetString("request_id"), body)
	if err != nil {
		httpx.Reject(c, http.StatusBadGateway, "proxy unavailable")
		return
	}
	c.Data(status, "application/json", respBody)
}

func main() {
	log := httpx.NewLogger("trusted-host")

	cfg, err := loadConfig()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}
	t := &trustedHost{cfg: cfg, client: &http.Client{Timeout: cfg.upstreamTimeout}, log: log}

	r := gin.New()
	_ = r.SetTrustedProxies(nil)
	r.Use(gin.Recovery(), httpx.RequestID(true), httpx.AccessLog(log))

	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.POST("/v1/execute", httpx.BearerAuth([][]byte{cfg.gatekeeperToken}), t.handleExecute)

	log.Info("config", "proxy", cfg.proxyURL)
	httpx.Serve(log, cfg.listenAddr, r)
}
