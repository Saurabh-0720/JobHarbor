// Package queue implements a reliable, priority-aware, Redis-backed job queue.
//
// Redis layout (prefix "jh:"):
//
//	jh:jobs HASH id->json | jh:index ZSET by created_at | jh:ready ZSET (2-prio)*1e13+ms
//	jh:delayed ZSET "prio:id" by run_at | jh:running ZSET id by lease expiry
//	jh:dead ZSET (DLQ) | jh:idem HASH | jh:stats HASH | jh:nodes HASH | jh:events LIST
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	kJobs    = "jh:jobs"
	kIndex   = "jh:index"
	kReady   = "jh:ready"
	kDelayed = "jh:delayed"
	kRunning = "jh:running"
	kDead    = "jh:dead"
	kIdem    = "jh:idem"
	kStats   = "jh:stats"
	kNodes   = "jh:nodes"
	kEvents  = "jh:events"
	kLatWait = "jh:lat:wait" // recent queue-wait samples (ms)
	kLatRun  = "jh:lat:run"  // recent execution-time samples (ms)
	latKeep  = 2000
)

var (
	ErrNotFound = errors.New("job not found")
	ErrConflict = errors.New("job is in a state that does not allow this action")
)

// Atomically move the best ready job into the running set with a lease.
var claimScript = redis.NewScript(`
local r = redis.call('ZPOPMIN', KEYS[1], 1)
if #r == 0 then return false end
redis.call('ZADD', KEYS[2], ARGV[1], r[1])
return r[1]
`)

// Atomically move due delayed jobs into the ready set.
var promoteScript = redis.NewScript(`
local ms = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 200)
local n = 0
for _, m in ipairs(ms) do
  if redis.call('ZREM', KEYS[1], m) == 1 then
    local p = tonumber(string.sub(m, 1, 1))
    local id = string.sub(m, 3)
    redis.call('ZADD', KEYS[2], string.format('%.0f', (2 - p) * 10000000000000 + tonumber(ARGV[1])), id)
    n = n + 1
  end
end
return n
`)

type Config struct {
	Lease       time.Duration
	BackoffBase time.Duration
	BackoffMax  time.Duration
	MaxJobsKept int
}

func DefaultConfig() Config {
	return Config{Lease: 45 * time.Second, BackoffBase: 2 * time.Second, BackoffMax: 60 * time.Second, MaxJobsKept: 5000}
}

type Queue struct {
	rdb *redis.Client
	cfg Config
}

func New(rdb *redis.Client, cfg Config) *Queue { return &Queue{rdb: rdb, cfg: cfg} }
func (q *Queue) Config() Config                { return q.cfg }

type EnqueueRequest struct {
	Type           string
	Payload        map[string]any
	Priority       Priority
	MaxRetries     int
	RunAt          time.Time
	IdempotencyKey string
}

func readyScore(p Priority, t time.Time) float64 {
	return float64(2-int(p))*1e13 + float64(t.UnixMilli())
}

func delayedMember(j *Job) string { return fmt.Sprintf("%d:%s", int(j.Priority), j.ID) }

// Enqueue stores a job. created=false means an existing job with the same idempotency key was returned.
func (q *Queue) Enqueue(ctx context.Context, r EnqueueRequest) (job *Job, created bool, err error) {
	now := time.Now()
	if r.Payload == nil {
		r.Payload = map[string]any{}
	}
	job = &Job{
		ID: newID(), Type: r.Type, Payload: r.Payload, Priority: r.Priority,
		MaxRetries: r.MaxRetries, IdempotencyKey: r.IdempotencyKey,
		CreatedAt: now, UpdatedAt: now, RunAt: now, Status: StatusQueued,
	}
	if !r.RunAt.IsZero() && r.RunAt.After(now) {
		job.RunAt = r.RunAt
		job.Status = StatusScheduled
	}
	if r.IdempotencyKey != "" {
		ok, err := q.rdb.HSetNX(ctx, kIdem, r.IdempotencyKey, job.ID).Result()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			id, err := q.rdb.HGet(ctx, kIdem, r.IdempotencyKey).Result()
			if err != nil {
				return nil, false, err
			}
			if existing, err := q.Get(ctx, id); err == nil {
				return existing, false, nil
			}
			q.rdb.HSet(ctx, kIdem, r.IdempotencyKey, job.ID) // stale key: take over
		}
	}
	data, _ := json.Marshal(job)
	pipe := q.rdb.TxPipeline()
	pipe.HSet(ctx, kJobs, job.ID, data)
	pipe.ZAdd(ctx, kIndex, redis.Z{Score: float64(now.UnixMilli()), Member: job.ID})
	if job.Status == StatusScheduled {
		pipe.ZAdd(ctx, kDelayed, redis.Z{Score: float64(job.RunAt.UnixMilli()), Member: delayedMember(job)})
	} else {
		pipe.ZAdd(ctx, kReady, redis.Z{Score: readyScore(job.Priority, now), Member: job.ID})
	}
	pipe.HIncrBy(ctx, kStats, "enqueued", 1)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, false, err
	}
	q.Event(ctx, "info", job.ID, fmt.Sprintf("enqueued %s (%s priority)", job.Type, prioName(job.Priority)))
	q.trim(ctx)
	return job, true, nil
}

func prioName(p Priority) string { return [...]string{"low", "normal", "high"}[p] }

func (q *Queue) trim(ctx context.Context) {
	n, err := q.rdb.ZCard(ctx, kIndex).Result()
	if err != nil || n <= int64(q.cfg.MaxJobsKept) {
		return
	}
	ids, err := q.rdb.ZRange(ctx, kIndex, 0, 499).Result()
	if err != nil || len(ids) == 0 {
		return
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	q.rdb.ZRem(ctx, kIndex, args...)
	q.rdb.HDel(ctx, kJobs, ids...)
}

func (q *Queue) Get(ctx context.Context, id string) (*Job, error) {
	s, err := q.rdb.HGet(ctx, kJobs, id).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var j Job
	if err := json.Unmarshal([]byte(s), &j); err != nil {
		return nil, err
	}
	return &j, nil
}

func (q *Queue) save(ctx context.Context, j *Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return q.rdb.HSet(ctx, kJobs, j.ID, data).Err()
}

type ListFilter struct {
	Status, Type, Query string
	Limit               int
}

func (q *Queue) List(ctx context.Context, f ListFilter) ([]*Job, error) {
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 100
	}
	key := kIndex
	if f.Status == string(StatusDead) {
		key = kDead
	}
	ids, err := q.rdb.ZRevRange(ctx, key, 0, 999).Result()
	if err != nil || len(ids) == 0 {
		return []*Job{}, err
	}
	vals, err := q.rdb.HMGet(ctx, kJobs, ids...).Result()
	if err != nil {
		return nil, err
	}
	needle := strings.ToLower(f.Query)
	out := make([]*Job, 0, f.Limit)
	for _, v := range vals {
		s, ok := v.(string)
		if !ok {
			continue
		}
		var j Job
		if json.Unmarshal([]byte(s), &j) != nil {
			continue
		}
		if f.Status != "" && f.Status != "all" && string(j.Status) != f.Status {
			continue
		}
		if f.Type != "" && j.Type != f.Type {
			continue
		}
		if needle != "" && !strings.Contains(strings.ToLower(j.ID+" "+j.Type+" "+j.Error+" "+j.IdempotencyKey), needle) {
			continue
		}
		out = append(out, &j)
		if len(out) >= f.Limit {
			break
		}
	}
	return out, nil
}

// Claim atomically takes the best ready job and leases it. (nil,nil) = empty.
func (q *Queue) Claim(ctx context.Context) (*Job, error) {
	for {
		lease := time.Now().Add(q.cfg.Lease).UnixMilli()
		res, err := claimScript.Run(ctx, q.rdb, []string{kReady, kRunning}, strconv.FormatInt(lease, 10)).Result()
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		id, _ := res.(string)
		j, err := q.Get(ctx, id)
		if errors.Is(err, ErrNotFound) {
			q.rdb.ZRem(ctx, kRunning, id)
			continue
		}
		return j, err
	}
}

func (q *Queue) MarkRunning(ctx context.Context, j *Job, worker string) error {
	now := time.Now()
	j.Attempts++
	j.Status = StatusRunning
	j.Error = ""
	j.UpdatedAt = now
	j.History = append(j.History, Attempt{N: j.Attempts, Worker: worker, StartedAt: now})
	if len(j.History) > maxHistory {
		j.History = j.History[len(j.History)-maxHistory:]
	}
	if wait := now.Sub(j.RunAt).Milliseconds(); wait >= 0 { // time spent ready-but-unclaimed
		pipe := q.rdb.Pipeline()
		pipe.LPush(ctx, kLatWait, wait)
		pipe.LTrim(ctx, kLatWait, 0, latKeep-1)
		_, _ = pipe.Exec(ctx)
	}
	return q.save(ctx, j)
}

func (q *Queue) Complete(ctx context.Context, j *Job, result string) error {
	now := time.Now()
	j.closeAttempt(now, "")
	j.Status, j.Result, j.Error, j.UpdatedAt, j.FinishedAt = StatusSucceeded, result, "", now, &now
	data, _ := json.Marshal(j)
	pipe := q.rdb.TxPipeline()
	pipe.ZRem(ctx, kRunning, j.ID)
	pipe.HSet(ctx, kJobs, j.ID, data)
	pipe.HIncrBy(ctx, kStats, "succeeded", 1)
	pipe.HIncrBy(ctx, kStats, "ms_total", j.DurationMs)
	pipe.LPush(ctx, kLatRun, j.DurationMs)
	pipe.LTrim(ctx, kLatRun, 0, latKeep-1)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	q.Event(ctx, "ok", j.ID, fmt.Sprintf("%s succeeded in %dms", j.Type, j.DurationMs))
	return nil
}

// Fail records a failed attempt: retry with exponential backoff, or DLQ when exhausted/permanent.
func (q *Queue) Fail(ctx context.Context, j *Job, cause error) error {
	now := time.Now()
	msg := cause.Error()
	j.closeAttempt(now, msg)
	j.Error = msg
	j.UpdatedAt = now

	pipe := q.rdb.TxPipeline()
	pipe.ZRem(ctx, kRunning, j.ID)
	pipe.HIncrBy(ctx, kStats, "failed", 1)
	var ev string
	level := "warn"
	if IsPermanent(cause) || j.Attempts > j.MaxRetries {
		j.Status = StatusDead
		j.FinishedAt = &now
		pipe.ZAdd(ctx, kDead, redis.Z{Score: float64(now.UnixMilli()), Member: j.ID})
		pipe.HIncrBy(ctx, kStats, "dead", 1)
		ev = fmt.Sprintf("%s moved to dead-letter queue: %s", j.Type, msg)
		level = "error"
	} else {
		d := q.backoff(j.Attempts)
		j.Status = StatusRetrying
		j.RunAt = now.Add(d)
		pipe.ZAdd(ctx, kDelayed, redis.Z{Score: float64(j.RunAt.UnixMilli()), Member: delayedMember(j)})
		pipe.HIncrBy(ctx, kStats, "retried", 1)
		ev = fmt.Sprintf("%s failed (attempt %d/%d), retrying in %s: %s", j.Type, j.Attempts, j.MaxRetries+1, d.Round(100*time.Millisecond), msg)
	}
	data, _ := json.Marshal(j)
	pipe.HSet(ctx, kJobs, j.ID, data)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	q.Event(ctx, level, j.ID, ev)
	return nil
}

func (q *Queue) backoff(attempt int) time.Duration {
	d := q.cfg.BackoffMax
	if attempt < 16 {
		if x := q.cfg.BackoffBase << uint(attempt-1); x < d {
			d = x
		}
	}
	return time.Duration(float64(d) * (0.8 + rand.Float64()*0.4)) // ±20% jitter
}

func (q *Queue) Promote(ctx context.Context) (int, error) {
	return promoteScript.Run(ctx, q.rdb, []string{kDelayed, kReady}, strconv.FormatInt(time.Now().UnixMilli(), 10)).Int()
}

// Reap re-queues jobs whose lease expired (worker crashed or hung).
func (q *Queue) Reap(ctx context.Context) (int, error) {
	ids, err := q.rdb.ZRangeByScore(ctx, kRunning, &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(time.Now().UnixMilli(), 10), Count: 100}).Result()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		if removed, err := q.rdb.ZRem(ctx, kRunning, id).Result(); err != nil || removed != 1 {
			continue
		}
		j, err := q.Get(ctx, id)
		if err != nil {
			continue
		}
		if q.Fail(ctx, j, errors.New("lease expired: worker crashed or timed out")) == nil {
			n++
		}
	}
	return n, nil
}

func (q *Queue) Cancel(ctx context.Context, id string) (*Job, error) {
	j, err := q.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	a, _ := q.rdb.ZRem(ctx, kReady, id).Result()
	b, _ := q.rdb.ZRem(ctx, kDelayed, delayedMember(j)).Result()
	if a+b == 0 {
		return nil, ErrConflict
	}
	now := time.Now()
	j.Status, j.UpdatedAt, j.FinishedAt = StatusCancelled, now, &now
	if err := q.save(ctx, j); err != nil {
		return nil, err
	}
	q.Event(ctx, "warn", id, j.Type+" cancelled")
	return j, nil
}

func (q *Queue) Retry(ctx context.Context, id string) (*Job, error) {
	j, err := q.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	switch j.Status {
	case StatusDead, StatusCancelled, StatusSucceeded:
	default:
		return nil, ErrConflict
	}
	now := time.Now()
	j.Attempts, j.Status, j.Error, j.Result = 0, StatusQueued, "", ""
	j.UpdatedAt, j.RunAt, j.FinishedAt = now, now, nil
	data, _ := json.Marshal(j)
	pipe := q.rdb.TxPipeline()
	pipe.ZRem(ctx, kDead, id)
	pipe.HSet(ctx, kJobs, id, data)
	pipe.ZAdd(ctx, kReady, redis.Z{Score: readyScore(j.Priority, now), Member: id})
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	q.Event(ctx, "info", id, j.Type+" re-queued manually")
	return j, nil
}

func (q *Queue) RetryAllDead(ctx context.Context) (int, error) {
	ids, err := q.rdb.ZRange(ctx, kDead, 0, 999).Result()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, id := range ids {
		if _, err := q.Retry(ctx, id); err == nil {
			n++
		}
	}
	return n, nil
}

func (q *Queue) Flush(ctx context.Context) error {
	var cursor uint64
	for {
		keys, next, err := q.rdb.Scan(ctx, cursor, "jh:*", 200).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := q.rdb.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		if cursor = next; cursor == 0 {
			return nil
		}
	}
}

type Depths struct {
	Ready   int64 `json:"ready"`
	Delayed int64 `json:"delayed"`
	Running int64 `json:"running"`
	Dead    int64 `json:"dead"`
}

func (q *Queue) Depths(ctx context.Context) (Depths, error) {
	pipe := q.rdb.Pipeline()
	r, d, run, dead := pipe.ZCard(ctx, kReady), pipe.ZCard(ctx, kDelayed), pipe.ZCard(ctx, kRunning), pipe.ZCard(ctx, kDead)
	if _, err := pipe.Exec(ctx); err != nil {
		return Depths{}, err
	}
	return Depths{r.Val(), d.Val(), run.Val(), dead.Val()}, nil
}

func (q *Queue) Counters(ctx context.Context) (map[string]int64, error) {
	m, err := q.rdb.HGetAll(ctx, kStats).Result()
	if err != nil {
		return nil, err
	}
	out := map[string]int64{"enqueued": 0, "succeeded": 0, "failed": 0, "retried": 0, "dead": 0, "ms_total": 0}
	for k, v := range m {
		out[k], _ = strconv.ParseInt(v, 10, 64)
	}
	return out, nil
}

type Node struct {
	ID          string    `json:"id"`
	Host        string    `json:"host"`
	Concurrency int       `json:"concurrency"`
	Inflight    int64     `json:"inflight"`
	Processed   int64     `json:"processed"`
	Failed      int64     `json:"failed"`
	StartedAt   time.Time `json:"started_at"`
	LastSeen    time.Time `json:"last_seen"`
}

func (q *Queue) Heartbeat(ctx context.Context, n Node) error {
	n.LastSeen = time.Now()
	data, _ := json.Marshal(n)
	return q.rdb.HSet(ctx, kNodes, n.ID, data).Err()
}

func (q *Queue) RemoveNode(ctx context.Context, id string) { q.rdb.HDel(ctx, kNodes, id) }

func (q *Queue) Nodes(ctx context.Context) ([]Node, error) {
	m, err := q.rdb.HGetAll(ctx, kNodes).Result()
	if err != nil {
		return nil, err
	}
	out := []Node{}
	for id, s := range m {
		var n Node
		if json.Unmarshal([]byte(s), &n) != nil {
			continue
		}
		if time.Since(n.LastSeen) > 8*time.Second {
			q.rdb.HDel(ctx, kNodes, id)
			continue
		}
		out = append(out, n)
	}
	return out, nil
}

type Event struct {
	T     int64  `json:"t"`
	Level string `json:"level"`
	JobID string `json:"job_id,omitempty"`
	Msg   string `json:"msg"`
}

func (q *Queue) Event(ctx context.Context, level, jobID, msg string) {
	data, _ := json.Marshal(Event{T: time.Now().UnixMilli(), Level: level, JobID: jobID, Msg: msg})
	pipe := q.rdb.Pipeline()
	pipe.LPush(ctx, kEvents, data)
	pipe.LTrim(ctx, kEvents, 0, 199)
	_, _ = pipe.Exec(ctx)
}

func (q *Queue) Events(ctx context.Context, n int) ([]Event, error) {
	vals, err := q.rdb.LRange(ctx, kEvents, 0, int64(n-1)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Event, 0, len(vals))
	for _, s := range vals {
		var e Event
		if json.Unmarshal([]byte(s), &e) == nil {
			out = append(out, e)
		}
	}
	return out, nil
}

// Latency summarises the most recent samples (nearest-rank percentiles, in ms).
type Latency struct {
	Count int   `json:"count"`
	P50   int64 `json:"p50"`
	P90   int64 `json:"p90"`
	P99   int64 `json:"p99"`
	Max   int64 `json:"max"`
}

// Percentiles computes nearest-rank percentiles of samples (which it sorts).
func Percentiles(samples []int64) Latency {
	n := len(samples)
	if n == 0 {
		return Latency{}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	at := func(p float64) int64 {
		i := int(math.Ceil(p/100*float64(n))) - 1
		if i < 0 {
			i = 0
		}
		return samples[i]
	}
	return Latency{Count: n, P50: at(50), P90: at(90), P99: at(99), Max: samples[n-1]}
}

func (q *Queue) latency(ctx context.Context, key string) (Latency, error) {
	vals, err := q.rdb.LRange(ctx, key, 0, latKeep-1).Result()
	if err != nil {
		return Latency{}, err
	}
	s := make([]int64, 0, len(vals))
	for _, v := range vals {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			s = append(s, n)
		}
	}
	return Percentiles(s), nil
}

// Latencies returns queue-wait (ready -> started) and run-time (handler) percentiles.
func (q *Queue) Latencies(ctx context.Context) (wait, run Latency, err error) {
	if wait, err = q.latency(ctx, kLatWait); err != nil {
		return
	}
	run, err = q.latency(ctx, kLatRun)
	return
}