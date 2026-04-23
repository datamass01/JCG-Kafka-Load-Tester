package metrics

import "time"

type Snapshot struct {
	TotalMessagesSent int64     `json:"total_messages_sent"`
	TotalErrors       int64     `json:"total_errors"`
	TotalBytesSent    int64     `json:"total_bytes_sent"`
	MsgPerSec         float64   `json:"msg_per_sec"`
	MBPerSec          float64   `json:"mb_per_sec"`
	ErrorRatePct      float64   `json:"error_rate_pct"`
	LatencyP50Ms      float64   `json:"latency_p50_ms"`
	LatencyP95Ms      float64   `json:"latency_p95_ms"`
	LatencyP99Ms      float64   `json:"latency_p99_ms"`
	LastError         string    `json:"last_error,omitempty"`
	Timestamp         time.Time `json:"timestamp"`
}
