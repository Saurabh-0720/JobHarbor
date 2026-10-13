// Package api exposes the HTTP/JSON API, Prometheus metrics and the dashboard.
package api

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"jobharbor/internal/queue"
	"jobharbor/internal/worker"
)

//go:embed web/*
var webFS embed.FS

type Sample struct {
	T    int64 `json:"t"`
	Enq  int64 `json:"enq"`
	OK   int64 `json:"ok"`
	Fail int64 `json:"fail"`
}

type Server struct {
	q         *queue.Queue
	apiKey    string
	redisMode string
	started   time.Time

	mu   sync.RWMutex
	hist []Sample
	last map[string]int64
}

func New(q *queue.Queue, apiKey, redisMode string) *Server {
	return &Server{q: q, apiKey: apiKey, redisMode: redisMode, started: time.Now()}
}

// StartSampler records per-second throughput so the dashboard has history.
func (s *Server) StartSampler(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c, err := s.q.Counters(ctx)
			if err != nil {
				continue
			}
			s.mu.Lock()
			if s.last != nil {
				d := func(k string) int64 {
					if v := c[k] - s.last[k]; v > 0 {
						return v
					}
					return 0
				}
				s.hist = append(s.hist, Sample{T: time.Now().UnixMilli(), Enq: d("enqueued"), OK: d("succeeded"), Fail: d("failed")})
				if len(s.hist) > 180 {
					s.hist = s.hist[len(s.hist)-180:]
				}
			}
			s.last = c
			s.mu.Unlock()
		}
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/jobs", s.enqueue)
	mux.HandleFunc("GET /api/jobs", s.list)
	mux.HandleFunc("GET /api/jobs/{id}", s.get)
	mux.HandleFunc("POST /api/jobs/{id}/retry", s.retry)
	mux.HandleFunc("DELETE /api/jobs/{id}", s.cancel)
	mux.HandleFunc("POST /api/dead/retry", s.retryDead)
	mux.HandleFunc("GET /api/stats", s.stats)
	mux.HandleFunc("GET /api/events", s.events)
	mux.HandleFunc("POST /api/demo", s.demo)
	mux.HandleFunc("POST /api/reset", s.reset)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	sub, _ := fs.Sub(webFS, "web")
	mux.Handle("GET /", http.FileServer(http.FS(sub)))
	return s.auth(mux)
}

// auth protects every write endpoint when an API key is configured.
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.apiKey != "" && r.Method != http.MethodGet && r.Method != http.MethodHead {
			if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-API-Key")), []byte(s.apiKey)) != 1 {
				writeErr(w, http.StatusUnauthorized, "missing or invalid X-API-Key header")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func mapErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrNotFound):
		writeErr(w, http.StatusNotFound, err.Error())
	case errors.Is(err, queue.ErrConflict):
		writeErr(w, http.StatusConflict, "job has already started or finished and can't be changed this way")
	default:
		writeErr(w, http.StatusInternalServerError, err.Error())
	}
}

type enqueueReq struct {
	Type           string         `json:"type"`
	Payload        map[string]any `json:"payload"`
	Priority       any            `json:"priority"`
	MaxRetries     *int           `json:"max_retries"`
	Retries        *int           `json:"retries"` // compatible with the original WorkQueue format
	DelaySeconds   float64        `json:"delay_seconds"`
	RunAt          *time.Time     `json:"run_at"`
	IdempotencyKey string         `json:"idempotency_key"`
}

func validType(t string) bool {
	for _, x := range worker.Types() {
		if x == t {
			return true
		}
	}
	return false
}

func (s *Server) toRequest(in enqueueReq) (queue.EnqueueRequest, error) {
	if !validType(in.Type) {
		return queue.EnqueueRequest{}, fmt.Errorf("unknown job type %q (supported: %v)", in.Type, worker.Types())
	}
	prio, err := queue.ParsePriority(in.Priority)
	if err != nil {
		return queue.EnqueueRequest{}, err
	}
	retries := 3
	if in.MaxRetries != nil {
		retries = *in.MaxRetries
	} else if in.Retries != nil {
		retries = *in.Retries
	}
	if retries < 0 || retries > 20 {
		return queue.EnqueueRequest{}, errors.New("max_retries must be between 0 and 20")
	}
	if in.DelaySeconds < 0 || in.DelaySeconds > 30*86400 {
		return queue.EnqueueRequest{}, errors.New("delay_seconds must be between 0 and 2592000")
	}
	if len(in.IdempotencyKey) > 128 {
		return queue.EnqueueRequest{}, errors.New("idempotency_key too long")
	}
	req := queue.EnqueueRequest{Type: in.Type, Payload: in.Payload, Priority: prio, MaxRetries: retries, IdempotencyKey: in.IdempotencyKey}
	if in.RunAt != nil {
		req.RunAt = *in.RunAt
	} else if in.DelaySeconds > 0 {
		req.RunAt = time.Now().Add(time.Duration(in.DelaySeconds * float64(time.Second)))
	}
	return req, nil
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var in enqueueReq
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	req, err := s.toRequest(in)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	job, created, err := s.q.Enqueue(r.Context(), req)
	if err != nil {
		mapErr(w, err)
		return
	}
	code := http.StatusCreated
	if !created {
		code = http.StatusOK
	}
	writeJSON(w, code, map[string]any{"job": job, "duplicate": !created})
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	qs := r.URL.Query()
	limit, _ := strconv.Atoi(qs.Get("limit"))
	jobs, err := s.q.List(r.Context(), queue.ListFilter{Status: qs.Get("status"), Type: qs.Get("type"), Query: qs.Get("q"), Limit: limit})
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"jobs": jobs})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	j, err := s.q.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, 200, j)
}

func (s *Server) retry(w http.ResponseWriter, r *http.Request) {
	j, err := s.q.Retry(r.Context(), r.PathValue("id"))
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, 200, j)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	j, err := s.q.Cancel(r.Context(), r.PathValue("id"))
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, 200, j)
}

func (s *Server) retryDead(w http.ResponseWriter, r *http.Request) {
	n, err := s.q.RetryAllDead(r.Context())
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]int{"requeued": n})
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	depths, err := s.q.Depths(r.Context())
	if err != nil {
		mapErr(w, err)
		return
	}
	counters, _ := s.q.Counters(r.Context())
	nodes, _ := s.q.Nodes(r.Context())
	wait, run, _ := s.q.Latencies(r.Context())
	s.mu.RLock()
	hist := append([]Sample(nil), s.hist...)
	s.mu.RUnlock()
	writeJSON(w, 200, map[string]any{
		"depths": depths, "counters": counters, "nodes": nodes, "history": hist,
		"latency": map[string]queue.Latency{"wait": wait, "run": run},
		"types":   worker.Types(), "redis": s.redisMode, "uptime_sec": int(time.Since(s.started).Seconds()),
		"server_time": time.Now().UnixMilli(), "auth_required": s.apiKey != "",
	})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	ev, err := s.q.Events(r.Context(), 60)
	if err != nil {
		mapErr(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"events": ev})
}

// demo enqueues a realistic mix of jobs, including ones that fail on purpose,
// so retries, backoff and the dead-letter queue are visible in the dashboard.
// With "spread_seconds" the jobs are enqueued gradually in the background,
// which produces a realistic throughput curve instead of one huge spike.
func (s *Server) demo(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Count  int  `json:"count"`
		Spread int  `json:"spread_seconds"`
		Clean  bool `json:"clean"` // true = no deliberately failing jobs
	}
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&in)
	if in.Count <= 0 {
		in.Count = 50
	}
	if in.Count > 2000 {
		in.Count = 2000
	}
	if in.Spread < 0 || in.Spread > 120 {
		in.Spread = 0
	}
	prios := []queue.Priority{queue.Low, queue.Normal, queue.Normal, queue.Normal, queue.High}
	one := func(ctx context.Context) error {
		req := queue.EnqueueRequest{Priority: prios[rand.Intn(len(prios))], MaxRetries: 3}
		n := rand.Intn(100)
		if in.Clean {
			n = rand.Intn(86) // skips the flaky and deliberately-failing ranges below
			if n >= 60 && n < 78 {
				n -= 18
			}
		}
		switch {
		case n < 30:
			req.Type, req.Payload = "send_email", map[string]any{"to": fmt.Sprintf("user%d@example.com", rand.Intn(9999)), "subject": "Welcome aboard"}
		case n < 45:
			req.Type, req.Payload = "resize_image", map[string]any{"width": 200 + rand.Intn(1800), "height": 200 + rand.Intn(1200)}
		case n < 60:
			req.Type, req.Payload = "generate_pdf", map[string]any{"title": fmt.Sprintf("Invoice #%d", 1000+rand.Intn(9000))}
		case n < 78:
			req.Type, req.Payload = "flaky", map[string]any{"fail_rate": 0.65}
		case n < 86:
			req.Type, req.Payload = "sleep", map[string]any{"seconds": 1 + rand.Intn(4)}
		case n < 93:
			req.Type, req.Payload = "send_email", map[string]any{"to": "not-an-email"} // permanent -> DLQ
		default:
			req.Type, req.Payload = "send_email", map[string]any{"to": "ops@example.com", "simulate_failure": true} // exhausts retries -> DLQ
			req.MaxRetries = 2
		}
		if rand.Intn(10) == 0 {
			req.RunAt = time.Now().Add(time.Duration(5+rand.Intn(20)) * time.Second)
		}
		_, _, err := s.q.Enqueue(ctx, req)
		return err
	}
	if in.Spread > 0 {
		go func() { // paced in the background, 10 ticks per second
			ctx, ticks := context.Background(), in.Spread*10
			t := time.NewTicker(100 * time.Millisecond)
			defer t.Stop()
			sent := 0
			for i := 1; i <= ticks; i++ {
				<-t.C
				for ; sent < in.Count*i/ticks; sent++ {
					if one(ctx) != nil {
						return
					}
				}
			}
		}()
		writeJSON(w, 202, map[string]int{"enqueued": 0, "scheduled": in.Count, "spread_seconds": in.Spread})
		return
	}
	for i := 0; i < in.Count; i++ {
		if err := one(r.Context()); err != nil {
			mapErr(w, err)
			return
		}
	}
	writeJSON(w, 200, map[string]int{"enqueued": in.Count})
}

func (s *Server) reset(w http.ResponseWriter, r *http.Request) {
	if err := s.q.Flush(r.Context()); err != nil {
		mapErr(w, err)
		return
	}
	s.mu.Lock()
	s.hist, s.last = nil, nil
	s.mu.Unlock()
	writeJSON(w, 200, map[string]bool{"ok": true})
}

// metrics exposes Prometheus text format.
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	d, err := s.q.Depths(r.Context())
	if err != nil {
		mapErr(w, err)
		return
	}
	c, _ := s.q.Counters(r.Context())
	nodes, _ := s.q.Nodes(r.Context())
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# TYPE jobharbor_queue_depth gauge\n")
	fmt.Fprintf(w, "jobharbor_queue_depth{state=\"ready\"} %d\njobharbor_queue_depth{state=\"scheduled\"} %d\njobharbor_queue_depth{state=\"running\"} %d\njobharbor_queue_depth{state=\"dead\"} %d\n", d.Ready, d.Delayed, d.Running, d.Dead)
	fmt.Fprintf(w, "# TYPE jobharbor_jobs_total counter\n")
	for _, k := range []string{"enqueued", "succeeded", "failed", "retried", "dead"} {
		fmt.Fprintf(w, "jobharbor_jobs_total{event=%q} %d\n", k, c[k])
	}
	wait, run, _ := s.q.Latencies(r.Context())
	fmt.Fprintf(w, "# HELP jobharbor_queue_wait_ms ready->started latency over the last 2000 jobs\n# TYPE jobharbor_queue_wait_ms gauge\n")
	fmt.Fprintf(w, "jobharbor_queue_wait_ms{quantile=\"0.5\"} %d\njobharbor_queue_wait_ms{quantile=\"0.9\"} %d\njobharbor_queue_wait_ms{quantile=\"0.99\"} %d\n", wait.P50, wait.P90, wait.P99)
	fmt.Fprintf(w, "# HELP jobharbor_run_ms handler execution time over the last 2000 successes\n# TYPE jobharbor_run_ms gauge\n")
	fmt.Fprintf(w, "jobharbor_run_ms{quantile=\"0.5\"} %d\njobharbor_run_ms{quantile=\"0.9\"} %d\njobharbor_run_ms{quantile=\"0.99\"} %d\n", run.P50, run.P90, run.P99)
	fmt.Fprintf(w, "# TYPE jobharbor_worker_nodes gauge\njobharbor_worker_nodes %d\n", len(nodes))
}