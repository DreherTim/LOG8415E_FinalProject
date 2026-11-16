// Gatekeeper which is the only public-facing service
//
// Responsibilities:
//   - authenticate clients (Bearer API key)
//   - validate every request against the operation whitelist (internal/api)
//   - forward only validated requests to the private Trusted Host
//
// Configuration (environment varialbes):
//
//	LISTEN_ADDR          default ":8080"
//	TRUSTED_HOST_URL     e.g. "http://10.0.2.15:8081"
//	TRUSTED_HOST_TOKEN   shared secret Gatekeeper -> Trusted Host
//	CLIENT_API_KEYS      comma-separated list of accepted client keys
//	UPSTREAM_TIMEOUT     default "5s"
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/DreherTim/LOG8415E_FinalProject/internal/api"
	"github.com/DreherTim/LOG8415E_FinalProject/internal/httpx"
)

const maxRequestBody = 4 << 10 // 4 KiB is plenty for any whitelisted op

type config struct {
	listenAddr       string
	trustedHostURL   string
	trustedHostToken string
	clientKeys       [][]byte
	upstreamTimeout  time.Duration
}

func loadConfig() (config, error) {
	cfg := config{
		listenAddr:       httpx.Getenv("LISTEN_ADDR", ":8080"),
		trustedHostURL:   strings.TrimRight(os.Getenv("TRUSTED_HOST_URL"), "/"),
		trustedHostToken: os.Getenv("TRUSTED_HOST_TOKEN"),
		clientKeys:       httpx.ParseKeys(os.Getenv("CLIENT_API_KEYS")),
	}
	if cfg.trustedHostURL == "" || cfg.trustedHostToken == "" {
		return cfg, errors.New("TRUSTED_HOST_URL and TRUSTED_HOST_TOKEN are required")
	}
	if len(cfg.clientKeys) == 0 {
		return cfg, errors.New("CLIENT_API_KEYS is required")
	}
	d, err := time.ParseDuration(httpx.Getenv("UPSTREAM_TIMEOUT", "5s"))
	if err != nil {
		return cfg, errors.New("UPSTREAM_TIMEOUT: " + err.Error())
	}
	cfg.upstreamTimeout = d
	return cfg, nil
}

// What the public client sends
type queryRequest struct {
	Op     string          `json:"op"`
	Params json.RawMessage `json:"params"`
}

type gatekeeper struct {
	cfg    config
	client *http.Client
}

func (g *gatekeeper) handleQuery(c *gin.Context) {
	// 1. Policy header (validated against a fixed set).
	policy, ok := api.ParsePolicy(c.GetHeader("X-Policy"))
	if !ok {
		httpx.Reject(c, http.StatusBadRequest, "invalid X-Policy")
		return
	}
	c.Set("policy", string(policy))

	// 2. Body: size-limited, strict JSON, no unknown top-level fields.
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxRequestBody)
	var req queryRequest
	dec := json.NewDecoder(c.Request.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		httpx.Reject(c, http.StatusBadRequest, "malformed JSON body")
		return
	}
	c.Set("op", req.Op)

	// 3. Operation whitelist + typed parameter validation.
	kind, params, err := api.ParseOp(req.Op, req.Params)
	if err != nil {
		httpx.Reject(c, http.StatusBadRequest, err.Error())
		return
	}

	// 4. Forward a canonical re-encoding (never the client's raw bytes).
	canonical, _ := json.Marshal(params)
	body, _ := json.Marshal(api.ExecuteRequest{
		RequestID: c.GetString("request_id"),
		Op:        req.Op,
		Kind:      kind,
		Policy:    policy,
		Params:    canonical,
	})

	ctx, cancel := context.WithTimeout(c.Request.Context(), g.cfg.upstreamTimeout)
	defer cancel()
	status, respBody, err := httpx.Forward(ctx, g.client,
		g.cfg.trustedHostURL+"/v1/execute", g.cfg.trustedHostToken, c.GetString("request_id"), body)
	if err != nil {
		// Don't leak internal addresses or error details to the public client.
		httpx.Reject(c, http.StatusBadGateway, "upstream unavailable")
		return
	}
	c.Data(status, "application/json", respBody)
}

func main() {
	log := httpx.NewLogger("gatekeeper")

	cfg, err := loadConfig()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}
	g := &gatekeeper{cfg: cfg, client: &http.Client{Timeout: cfg.upstreamTimeout}}

	r := gin.New()
	_ = r.SetTrustedProxies(nil) // don't trust X-Forwarded-For from the Internet
	r.Use(gin.Recovery(), httpx.RequestID(false), httpx.AccessLog(log))

	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.POST("/v1/query", httpx.BearerAuth(cfg.clientKeys), g.handleQuery)

	log.Info("config", "trusted_host", cfg.trustedHostURL)
	httpx.Serve(log, cfg.listenAddr, r)
}
