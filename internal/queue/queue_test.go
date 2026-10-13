package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestQueue(t *testing.T) *Queue {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	cfg := DefaultConfig()
	cfg.BackoffBase, cfg.BackoffMax = 10*time.Millisecond, 50*time.Millisecond
	return New(rdb, cfg)
}

func TestPriorityThenFIFO(t *testing.T) {
	q, ctx := newTestQueue(t), context.Background()
	mk := func(p Priority) *Job {
		j, _, err := q.Enqueue(ctx, EnqueueRequest{Type: "sleep", Priority: p})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
		return j
	}
	low, n1, high, n2 := mk(Low), mk(Normal), mk(High), mk(Normal)
	for i, want := range []*Job{high, n1, n2, low} {
		got, err := q.Claim(ctx)
		if err != nil || got == nil || got.ID != want.ID {
			t.Fatalf("claim %d: got %+v err %v, want %s", i, got, err, want.ID)
		}
	}
	if j, _ := q.Claim(ctx); j != nil {
		t.Fatal("expected empty queue")
	}
}

func TestRetryBackoffThenDead(t *testing.T) {
	q, ctx := newTestQueue(t), context.Background()
	q.Enqueue(ctx, EnqueueRequest{Type: "flaky", MaxRetries: 2})
	for attempt := 1; attempt <= 3; attempt++ {
		var j *Job
		for i := 0; i < 100 && j == nil; i++ {
			q.Promote(ctx)
			j, _ = q.Claim(ctx)
			if j == nil {
				time.Sleep(10 * time.Millisecond)
			}
		}
		if j == nil {
			t.Fatalf("attempt %d never became ready", attempt)
		}
		q.MarkRunning(ctx, j, "w1")
		q.Fail(ctx, j, errors.New("boom"))
		got, _ := q.Get(ctx, j.ID)
		if attempt < 3 && got.Status != StatusRetrying || attempt == 3 && got.Status != StatusDead {
			t.Fatalf("attempt %d: status %s", attempt, got.Status)
		}
	}
	if d, _ := q.Depths(ctx); d.Dead != 1 {
		t.Fatalf("dead depth %d", d.Dead)
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	q, ctx := newTestQueue(t), context.Background()
	q.Enqueue(ctx, EnqueueRequest{Type: "send_email", MaxRetries: 5})
	j, _ := q.Claim(ctx)
	q.MarkRunning(ctx, j, "w")
	q.Fail(ctx, j, Permanent(errors.New("bad address")))
	if got, _ := q.Get(ctx, j.ID); got.Status != StatusDead || got.Attempts != 1 {
		t.Fatalf("job: %+v", got)
	}
}

func TestDelayedJobPromotion(t *testing.T) {
	q, ctx := newTestQueue(t), context.Background()
	j, _, _ := q.Enqueue(ctx, EnqueueRequest{Type: "sleep", RunAt: time.Now().Add(150 * time.Millisecond)})
	if c, _ := q.Claim(ctx); c != nil {
		t.Fatal("claimed before due")
	}
	time.Sleep(200 * time.Millisecond)
	if n, _ := q.Promote(ctx); n != 1 {
		t.Fatalf("promoted %d", n)
	}
	if c, _ := q.Claim(ctx); c == nil || c.ID != j.ID {
		t.Fatal("expected promoted job")
	}
}

func TestIdempotencyKey(t *testing.T) {
	q, ctx := newTestQueue(t), context.Background()
	a, c1, _ := q.Enqueue(ctx, EnqueueRequest{Type: "sleep", IdempotencyKey: "order-42"})
	b, c2, _ := q.Enqueue(ctx, EnqueueRequest{Type: "sleep", IdempotencyKey: "order-42"})
	if !c1 || c2 || a.ID != b.ID {
		t.Fatalf("created=%v/%v", c1, c2)
	}
}

func TestReaperRecoversCrashedWorker(t *testing.T) {
	q, ctx := newTestQueue(t), context.Background()
	q.cfg.Lease = 30 * time.Millisecond
	q.Enqueue(ctx, EnqueueRequest{Type: "sleep", MaxRetries: 1})
	j, _ := q.Claim(ctx) // worker "crashes" after claiming
	time.Sleep(60 * time.Millisecond)
	if n, _ := q.Reap(ctx); n != 1 {
		t.Fatalf("reaped %d", n)
	}
	if got, _ := q.Get(ctx, j.ID); got.Status != StatusRetrying || got.Attempts != 1 {
		t.Fatalf("job: %+v", got)
	}
}

func TestPercentiles(t *testing.T) {
	s := make([]int64, 100)
	for i := range s {
		s[i] = int64(100 - i) // unsorted 100..1
	}
	l := Percentiles(s)
	if l.P50 != 50 || l.P90 != 90 || l.P99 != 99 || l.Max != 100 || l.Count != 100 {
		t.Fatalf("got %+v", l)
	}
	if (Percentiles(nil) != Latency{}) {
		t.Fatal("empty input should give zero Latency")
	}
}

func TestLatencyRecorded(t *testing.T) {
	q, ctx := newTestQueue(t), context.Background()
	q.Enqueue(ctx, EnqueueRequest{Type: "sleep"})
	time.Sleep(20 * time.Millisecond)
	j, _ := q.Claim(ctx)
	q.MarkRunning(ctx, j, "w")
	q.Complete(ctx, j, "ok")
	wait, run, err := q.Latencies(ctx)
	if err != nil || wait.Count != 1 || run.Count != 1 || wait.P50 < 20 {
		t.Fatalf("wait=%+v run=%+v err=%v", wait, run, err)
	}
}