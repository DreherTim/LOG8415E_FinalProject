// Package httpx holds the HTTP plumbing shared by the Go services:
// request IDs, JSON access logs, bearer-token auth, forwarding, and
// graceful shutdown.
package httpx

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

const MaxUpstreamBody = 1 << 20 // cap on what we relay back

// ---------- env helpers ----------

func Getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ParseKeys splits a comma-separated list of secrets.
func ParseKeys(csv string) [][]byte {
	var keys [][]byte
	for _, k := range strings.Split(csv, ",") {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, []byte(k))
		}
	}
	return keys
}

// ---------- middleware ----------

var validID = regexp.MustCompile(`^[0-9a-f]{16}$`)

// RequestID tags every request so it can be traced across all hops.
// Internal services set trustIncoming=true to keep the ID chosen upstream;
// the public Gatekeeper sets false and always generates its own.
func RequestID(trustIncoming bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-ID")
		if !trustIncoming || !validID.MatchString(id) {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			id = hex.EncodeToString(b)
		}
		c.Set("request_id", id)
		c.Header("X-Request-ID", id)
		c.Next()
	}
}

// AccessLog writes one JSON line per request (easy to query in CloudWatch Logs Insights).
func AccessLog(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info("request",
			"request_id", c.GetString("request_id"),
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"latency_ms", float64(time.Since(start).Microseconds())/1000,
			"client_ip", c.ClientIP(),
			"op", c.GetString("op"),
			"policy", c.GetString("policy"),
			"node", c.GetString("node"),
			"reject_reason", c.GetString("reject_reason"),
		)
	}
}

// BearerAuth checks "Authorization: Bearer <key>" with a constant-time comparison.
func BearerAuth(keys [][]byte) gin.HandlerFunc {
	return func(c *gin.Context) {
		token, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer ")
		if ok {
			for _, k := range keys {
				if subtle.ConstantTimeCompare([]byte(token), k) == 1 {
					c.Next()
					return
				}
			}
		}
		Reject(c, http.StatusUnauthorized, "unauthorized")
	}
}

// Reject aborts with a JSON error and records the reason for the access log.
func Reject(c *gin.Context, status int, reason string) {
	c.Set("reject_reason", reason)
	c.AbortWithStatusJSON(status, gin.H{"error": reason, "request_id": c.GetString("request_id")})
}

// ---------- forwarding ----------

// Forward POSTs a JSON body to url with a bearer token and the request ID,
// and returns the upstream status and (size-capped) body.
func Forward(ctx context.Context, client *http.Client, url, token, requestID string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Request-ID", requestID)

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, MaxUpstreamBody))
	return resp.StatusCode, respBody, err
}

// ---------- server ----------

// Serve runs h on addr and shuts down gracefully on SIGINT/SIGTERM
// (systemd sends SIGTERM on stop).
func Serve(log *slog.Logger, addr string, h http.Handler) {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Info("stopped")
}

// NewLogger returns a JSON logger tagged with the service name.
func NewLogger(service string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", service)
}
