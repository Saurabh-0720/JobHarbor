package queue

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

type Status string

const (
	StatusQueued    Status = "queued"
	StatusScheduled Status = "scheduled"
	StatusRunning   Status = "running"
	StatusRetrying  Status = "retrying"
	StatusSucceeded Status = "succeeded"
	StatusDead      Status = "dead"
	StatusCancelled Status = "cancelled"
)

type Priority int

const (
	Low    Priority = 0
	Normal Priority = 1
	High   Priority = 2
)

func ParsePriority(v any) (Priority, error) {
	switch x := v.(type) {
	case nil:
		return Normal, nil
	case string:
		switch x {
		case "", "normal":
			return Normal, nil
		case "low":
			return Low, nil
		case "high":
			return High, nil
		}
	case float64:
		if x >= 0 && x <= 2 && x == float64(int(x)) {
			return Priority(int(x)), nil
		}
	}
	return Normal, fmt.Errorf("priority must be low, normal or high")
}

type Attempt struct {
	N         int        `json:"n"`
	Worker    string     `json:"worker,omitempty"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
	Error     string     `json:"error,omitempty"`
}

type Job struct {
	ID             string         `json:"id"`
	Type           string         `json:"type"`
	Payload        map[string]any `json:"payload"`
	Priority       Priority       `json:"priority"`
	MaxRetries     int            `json:"max_retries"`
	Attempts       int            `json:"attempts"`
	Status         Status         `json:"status"`
	Error          string         `json:"error,omitempty"`
	Result         string         `json:"result,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	RunAt          time.Time      `json:"run_at"`
	FinishedAt     *time.Time     `json:"finished_at,omitempty"`
	DurationMs     int64          `json:"duration_ms,omitempty"`
	History        []Attempt      `json:"history,omitempty"`
}

const maxHistory = 25

func newID() string {
	b := make([]byte, 7)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (j *Job) closeAttempt(now time.Time, errMsg string) {
	if n := len(j.History); n == 0 || j.History[n-1].EndedAt != nil {
		j.Attempts++
		j.History = append(j.History, Attempt{N: j.Attempts, StartedAt: now})
	}
	a := &j.History[len(j.History)-1]
	end := now
	a.EndedAt = &end
	a.Error = errMsg
	j.DurationMs = now.Sub(a.StartedAt).Milliseconds()
	if len(j.History) > maxHistory {
		j.History = j.History[len(j.History)-maxHistory:]
	}
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err so the job goes straight to the dead-letter queue.
func Permanent(err error) error { return &permanentError{err} }

func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
