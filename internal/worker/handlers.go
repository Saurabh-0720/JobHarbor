package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"jobharbor/internal/queue"
)

// Handler runs one job. Return queue.Permanent(err) for errors that must not be retried.
type Handler func(ctx context.Context, j *queue.Job) (string, error)

// handlers is the registry. To add a job type, add one line here.
var handlers = map[string]Handler{
	"send_email":   sendEmail,
	"resize_image": resizeImage,
	"generate_pdf": generatePDF,
	"webhook":      webhook,
	"flaky":        flaky,
	"sleep":        sleepJob,
}

func Types() []string {
	t := make([]string, 0, len(handlers))
	for k := range handlers {
		t = append(t, k)
	}
	sort.Strings(t)
	return t
}

func num(p map[string]any, key string, def float64) float64 {
	switch v := p[key].(type) {
	case float64:
		return v
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func str(p map[string]any, key string) string { s, _ := p[key].(string); return s }

func nap(ctx context.Context, min, max int) error {
	d := time.Duration(min+rand.Intn(max-min+1)) * time.Millisecond
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func sendEmail(ctx context.Context, j *queue.Job) (string, error) {
	to := str(j.Payload, "to")
	if !strings.Contains(to, "@") || strings.ContainsAny(to, " ,;") {
		return "", queue.Permanent(fmt.Errorf("invalid recipient %q", to))
	}
	if err := nap(ctx, 400, 1200); err != nil { // stand-in for SMTP / SES
		return "", err
	}
	if b, _ := j.Payload["simulate_failure"].(bool); b {
		return "", errors.New("smtp: 451 temporary failure, try again later")
	}
	return "delivered to " + to, nil
}

func resizeImage(ctx context.Context, j *queue.Job) (string, error) {
	w, h := num(j.Payload, "width", 0), num(j.Payload, "height", 0)
	if w < 1 || h < 1 || w > 10000 || h > 10000 {
		return "", queue.Permanent(errors.New("width and height must be between 1 and 10000"))
	}
	if err := nap(ctx, 600, 1500); err != nil {
		return "", err
	}
	return fmt.Sprintf("resized to %.0fx%.0f", w, h), nil
}

func generatePDF(ctx context.Context, j *queue.Job) (string, error) {
	title := str(j.Payload, "title")
	if title == "" {
		return "", queue.Permanent(errors.New("title is required"))
	}
	if err := nap(ctx, 1000, 2500); err != nil {
		return "", err
	}
	return fmt.Sprintf("%q.pdf generated (%d pages)", title, 1+rand.Intn(12)), nil
}

func flaky(ctx context.Context, j *queue.Job) (string, error) {
	if err := nap(ctx, 300, 800); err != nil {
		return "", err
	}
	if rand.Float64() < num(j.Payload, "fail_rate", 0.6) {
		return "", errors.New("upstream service unavailable (simulated)")
	}
	return "ok after " + strconv.Itoa(j.Attempts) + " attempt(s)", nil
}

func sleepJob(ctx context.Context, j *queue.Job) (string, error) {
	s := num(j.Payload, "seconds", 1)
	if s < 0 || s > 25 {
		return "", queue.Permanent(errors.New("seconds must be between 0 and 25"))
	}
	select {
	case <-time.After(time.Duration(s * float64(time.Second))):
		return fmt.Sprintf("slept %.1fs", s), nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// webhook performs a real HTTP POST. 5xx/429/network errors retry, 4xx do not.
// Private/loopback targets are blocked (SSRF) unless JH_ALLOW_PRIVATE_WEBHOOKS=1.
func webhook(ctx context.Context, j *queue.Job) (string, error) {
	raw := str(j.Payload, "url")
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", queue.Permanent(fmt.Errorf("invalid url %q", raw))
	}
	if os.Getenv("JH_ALLOW_PRIVATE_WEBHOOKS") != "1" {
		ips, err := net.LookupIP(u.Hostname())
		if err != nil {
			return "", fmt.Errorf("dns lookup failed: %w", err)
		}
		for _, ip := range ips {
			if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
				return "", queue.Permanent(errors.New("target resolves to a private address (blocked)"))
			}
		}
	}
	body, _ := json.Marshal(j.Payload["body"])
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, raw, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-JobHarbor-Job-ID", j.ID)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	switch {
	case resp.StatusCode >= 500 || resp.StatusCode == 429:
		return "", fmt.Errorf("remote returned HTTP %d", resp.StatusCode)
	case resp.StatusCode >= 400:
		return "", queue.Permanent(fmt.Errorf("remote rejected request: HTTP %d", resp.StatusCode))
	}
	return fmt.Sprintf("HTTP %d", resp.StatusCode), nil
}
