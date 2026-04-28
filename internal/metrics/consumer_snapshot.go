package metrics

import "time"

type ConsumerSnapshot struct {
	TotalMessagesConsumed int64     `json:"total_messages_consumed"`
	TotalErrors           int64     `json:"total_errors"`
	TotalBytesConsumed    int64     `json:"total_bytes_consumed"`
	MsgPerSec             float64   `json:"msg_per_sec"`
	MBPerSec              float64   `json:"mb_per_sec"`
	ErrorRatePct          float64   `json:"error_rate_pct"`
	LatencyP50Ms          float64   `json:"latency_p50_ms"`
	LatencyP95Ms          float64   `json:"latency_p95_ms"`
	LatencyP99Ms          float64   `json:"latency_p99_ms"`
	LagMessages           int64     `json:"lag_messages"`
	LastError             string    `json:"last_error,omitempty"`
	Timestamp             time.Time `json:"timestamp"`
}
