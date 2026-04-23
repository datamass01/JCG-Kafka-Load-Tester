package metrics

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Aggregator struct {
	mu           sync.Mutex
	totalSent    atomic.Int64
	totalBytes   atomic.Int64
	totalErrors  atomic.Int64
	latencies    []time.Duration
	windowStart  time.Time
	windowSent   int64
	windowBytes  int64
	stopped      bool
	stoppedAt    time.Time
	lastError    string
	lastErrorMu  sync.RWMutex
}

func NewAggregator() *Aggregator {
	return &Aggregator{windowStart: time.Now()}
}

func (a *Aggregator) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.totalSent.Store(0)
	a.totalBytes.Store(0)
	a.totalErrors.Store(0)
	a.latencies = a.latencies[:0]
	a.windowStart = time.Now()
	a.windowSent = 0
	a.windowBytes = 0
	a.stopped = false
}

func (a *Aggregator) RecordSuccess(latency time.Duration, bytes int) {
	a.totalSent.Add(1)
	a.totalBytes.Add(int64(bytes))

	a.mu.Lock()
	a.latencies = append(a.latencies, latency)
	// Keep only last 10k samples to bound memory
	if len(a.latencies) > 10000 {
		a.latencies = a.latencies[len(a.latencies)-10000:]
	}
	a.windowSent++
	a.windowBytes += int64(bytes)
	a.mu.Unlock()
}

func (a *Aggregator) RecordError(err error) {
	a.totalErrors.Add(1)
	if err != nil {
		a.lastErrorMu.Lock()
		a.lastError = err.Error()
		a.lastErrorMu.Unlock()
	}
}

func (a *Aggregator) MarkStopped() {
	a.mu.Lock()
	a.stopped = true
	a.stoppedAt = time.Now()
	a.mu.Unlock()
}

func (a *Aggregator) Snapshot() Snapshot {
	a.mu.Lock()
	now := time.Now()
	elapsed := now.Sub(a.windowStart).Seconds()
	msgPerSec := 0.0
	mbPerSec := 0.0
	if elapsed > 0 {
		msgPerSec = float64(a.windowSent) / elapsed
		mbPerSec = float64(a.windowBytes) / elapsed / 1024 / 1024
	}

	// Reset sliding window every 5 seconds
	if elapsed >= 5 {
		a.windowStart = now
		a.windowSent = 0
		a.windowBytes = 0
	}

	totalSent := a.totalSent.Load()
	totalErrors := a.totalErrors.Load()
	errorRate := 0.0
	if total := totalSent + totalErrors; total > 0 {
		errorRate = float64(totalErrors) / float64(total) * 100
	}

	p50, p95, p99 := latencyPercentiles(a.latencies)
	a.mu.Unlock()

	a.lastErrorMu.RLock()
	lastErr := a.lastError
	a.lastErrorMu.RUnlock()

	return Snapshot{
		TotalMessagesSent: totalSent,
		TotalErrors:       totalErrors,
		TotalBytesSent:    a.totalBytes.Load(),
		MsgPerSec:         msgPerSec,
		MBPerSec:          mbPerSec,
		ErrorRatePct:      errorRate,
		LatencyP50Ms:      p50,
		LatencyP95Ms:      p95,
		LatencyP99Ms:      p99,
		LastError:         lastErr,
		Timestamp:         now,
	}
}

func latencyPercentiles(samples []time.Duration) (p50, p95, p99 float64) {
	if len(samples) == 0 {
		return 0, 0, 0
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	idx := func(pct float64) time.Duration {
		i := int(float64(len(sorted)-1) * pct)
		return sorted[i]
	}
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }
	return ms(idx(0.50)), ms(idx(0.95)), ms(idx(0.99))
}
