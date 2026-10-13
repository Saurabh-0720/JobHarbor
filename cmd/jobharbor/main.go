package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"jobharbor/internal/api"
	"jobharbor/internal/queue"
	"jobharbor/internal/worker"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	mode := flag.String("mode", env("JH_MODE", "all"), "all | api | worker")
	addr := flag.String("addr", env("JH_ADDR", ":8080"), "HTTP listen address")
	redisAddr := flag.String("redis", env("JH_REDIS", ""), "Redis host:port or redis:// URL (empty = embedded in-memory Redis)")
	defWorkers, _ := strconv.Atoi(env("JH_WORKERS", "4"))
	workers := flag.Int("workers", defWorkers, "concurrent workers per node")
	flag.Parse()

	if *mode != "all" && *mode != "api" && *mode != "worker" {
		log.Fatalf("invalid -mode %q", *mode)
	}
	if *workers < 1 {
		*workers = 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisMode := *redisAddr
	if *redisAddr == "" {
		if *mode != "all" {
			log.Fatal("-mode " + *mode + " needs a shared Redis: set -redis or JH_REDIS")
		}
		mr, err := miniredis.Run()
		if err != nil {
			log.Fatalf("embedded redis: %v", err)
		}
		defer mr.Close()
		*redisAddr, redisMode = mr.Addr(), "embedded (in-memory, data lost on exit)"
		log.Println("no JH_REDIS set: using embedded in-memory Redis")
	} else {
		redisMode = "external"
	}

	var opts *redis.Options
	if strings.HasPrefix(*redisAddr, "redis://") || strings.HasPrefix(*redisAddr, "rediss://") {
		o, err := redis.ParseURL(*redisAddr)
		if err != nil {
			log.Fatalf("bad redis url: %v", err)
		}
		opts = o
	} else {
		opts = &redis.Options{Addr: *redisAddr, Password: os.Getenv("JH_REDIS_PASSWORD")}
	}
	rdb := redis.NewClient(opts)
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if err := rdb.Ping(pctx).Err(); err != nil {
		log.Fatalf("cannot reach redis at %s: %v", *redisAddr, err)
	}
	cancel()

	q := queue.New(rdb, queue.DefaultConfig())
	var wg sync.WaitGroup

	if *mode == "all" || *mode == "worker" {
		pool := worker.NewPool(q, *workers)
		wg.Add(1)
		go func() { defer wg.Done(); pool.Run(ctx) }()
	}
	if *mode == "all" || *mode == "api" {
		srv := api.New(q, os.Getenv("JH_API_KEY"), redisMode)
		go srv.StartSampler(ctx)
		hs := &http.Server{Addr: *addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ctx.Done()
			sctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			_ = hs.Shutdown(sctx)
		}()
		go func() {
			log.Printf("JobHarbor dashboard + API on http://localhost%s  (mode=%s)", *addr, *mode)
			if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("http: %v", err)
			}
		}()
	}
	<-ctx.Done()
	log.Println("shutting down: finishing in-flight jobs...")
	wg.Wait()
}
