// Package worker runs a pool of goroutines that pull jobs from the queue.
package worker

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"jobharbor/internal/queue"
)

const jobTimeout = 30 * time.Second

type Pool struct {
	q         *queue.Queue
	id        string
	n         int
	host      string
	started   time.Time
	inflight  atomic.Int64
	processed atomic.Int64
	failed    atomic.Int64
}

func NewPool(q *queue.Queue, concurrency int) *Pool {
	host, _ := os.Hostname()
	return &Pool{q: q, n: concurrency, host: host, started: time.Now(), id: fmt.Sprintf("%s-%04x", host, rand.Intn(0xffff))}
}

func (p *Pool) ID() string { return p.id }

// Run blocks until ctx is cancelled and in-flight jobs have finished (graceful shutdown).
func (p *Pool) Run(ctx context.Context) {
	bg := context.WithoutCancel(ctx)
	var workers, helpers sync.WaitGroup
	for i := 0; i < p.n; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			p.loop(ctx, bg, fmt.Sprintf("%s/w%d", p.id, i))
		}(i)
	}
	helperCtx, stopHelpers := context.WithCancel(bg)
	tick := func(every time.Duration, fn func()) {
		helpers.Add(1)
		go func() {
			defer helpers.Done()
			t := time.NewTicker(every)
			defer t.Stop()
			fn()
			for {
				select {
				case <-t.C:
					fn()
				case <-helperCtx.Done():
					return
				}
			}
		}()
	}
	tick(300*time.Millisecond, func() {
		if _, err := p.q.Promote(helperCtx); err != nil {
			log.Printf("promote: %v", err)
		}
	})
	tick(2*time.Second, func() {
		if n, err := p.q.Reap(helperCtx); err != nil {
			log.Printf("reap: %v", err)
		} else if n > 0 {
			log.Printf("reaped %d job(s) with expired leases", n)
		}
	})
	tick(2*time.Second, func() {
		_ = p.q.Heartbeat(helperCtx, queue.Node{ID: p.id, Host: p.host, Concurrency: p.n, StartedAt: p.started,
			Inflight: p.inflight.Load(), Processed: p.processed.Load(), Failed: p.failed.Load()})
	})
	log.Printf("worker node %s started with %d workers", p.id, p.n)
	workers.Wait()
	stopHelpers()
	helpers.Wait()
	p.q.RemoveNode(bg, p.id)
	log.Printf("worker node %s stopped cleanly", p.id)
}

func (p *Pool) loop(ctx, bg context.Context, wid string) {
	for ctx.Err() == nil {
		cctx, cancel := context.WithTimeout(bg, 5*time.Second)
		j, err := p.q.Claim(cctx)
		cancel()
		if err != nil {
			log.Printf("[%s] claim error: %v", wid, err)
			sleep(ctx, time.Second)
			continue
		}
		if j == nil {
			sleep(ctx, 120*time.Millisecond+time.Duration(rand.Intn(100))*time.Millisecond)
			continue
		}
		p.execute(bg, wid, j)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

func (p *Pool) execute(bg context.Context, wid string, j *queue.Job) {
	p.inflight.Add(1)
	defer p.inflight.Add(-1)
	if err := p.q.MarkRunning(bg, j, wid); err != nil {
		log.Printf("[%s] mark running %s: %v", wid, j.ID, err)
	}
	p.q.Event(bg, "info", j.ID, fmt.Sprintf("%s started %s (attempt %d)", wid, j.Type, j.Attempts))
	res, err := run(bg, j)
	if err != nil {
		p.failed.Add(1)
		log.Printf("[%s] job %s (%s) failed: %v", wid, j.ID, j.Type, err)
		if ferr := p.q.Fail(bg, j, err); ferr != nil {
			log.Printf("[%s] record failure %s: %v", wid, j.ID, ferr)
		}
		return
	}
	p.processed.Add(1)
	if cerr := p.q.Complete(bg, j, res); cerr != nil {
		log.Printf("[%s] record success %s: %v", wid, j.ID, cerr)
	}
}

// run executes the handler with a timeout and converts panics into errors.
func run(parent context.Context, j *queue.Job) (res string, err error) {
	h, ok := handlers[j.Type]
	if !ok {
		return "", queue.Permanent(fmt.Errorf("no handler registered for type %q", j.Type))
	}
	ctx, cancel := context.WithTimeout(parent, jobTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v\n%s", r, debug.Stack())
			if len(err.Error()) > 600 {
				err = fmt.Errorf("handler panic: %v", r)
			}
		}
	}()
	return h(ctx, j)
}
