package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type RunRecord struct {
	ID            string    `json:"id"`
	StartedAt     time.Time `json:"started_at"`
	StoppedAt     time.Time `json:"stopped_at"`
	Topic         string    `json:"topic"`
	Workers       int       `json:"workers"`
	TargetMsgSec  int       `json:"target_msg_per_sec"`
	MsgSizeBytes  int       `json:"message_size_bytes"`
	TotalSent     int64     `json:"total_messages_sent"`
	TotalErrors   int64     `json:"total_errors"`
	TotalBytes    int64     `json:"total_bytes_sent"`
	AvgMsgPerSec  float64   `json:"avg_msg_per_sec"`
	AvgMBPerSec   float64   `json:"avg_mb_per_sec"`
	AvgLatencyP99 float64   `json:"avg_latency_p99_ms"`
}

type ConsumerRunRecord struct {
	ID            string    `json:"id"`
	StartedAt     time.Time `json:"started_at"`
	StoppedAt     time.Time `json:"stopped_at"`
	Topic         string    `json:"topic"`
	ConsumerGroup string    `json:"consumer_group"`
	OffsetReset   string    `json:"offset_reset"`
	TotalConsumed int64     `json:"total_messages_consumed"`
	TotalErrors   int64     `json:"total_errors"`
	TotalBytes    int64     `json:"total_bytes_consumed"`
	AvgMsgPerSec  float64   `json:"avg_msg_per_sec"`
	AvgMBPerSec   float64   `json:"avg_mb_per_sec"`
	AvgLatencyP99 float64   `json:"avg_latency_p99_ms"`
}

type Store struct {
	dataDir        string
	rotationSizeMB int
	mu             sync.Mutex
	currentFile    *os.File
	consumerFile   *os.File
}

func NewStore(dataDir string, rotationSizeMB int) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	s := &Store{dataDir: dataDir, rotationSizeMB: rotationSizeMB}
	if err := s.openFile(); err != nil {
		return nil, err
	}
	if err := s.openConsumerFile(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) SaveRun(r RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.rotateIfNeeded(); err != nil {
		return err
	}

	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("marshal run: %w", err)
	}
	_, err = fmt.Fprintf(s.currentFile, "%s\n", data)
	return err
}

func (s *Store) SaveConsumerRun(r ConsumerRunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("marshal consumer run: %w", err)
	}
	_, err = fmt.Fprintf(s.consumerFile, "%s\n", data)
	return err
}

func (s *Store) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.currentFile != nil {
		s.currentFile.Close()
	}
	if s.consumerFile != nil {
		s.consumerFile.Close()
	}
}

func (s *Store) openFile() error {
	name := filepath.Join(s.dataDir, "runs.jsonl")
	f, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open runs file: %w", err)
	}
	if s.currentFile != nil {
		s.currentFile.Close()
	}
	s.currentFile = f
	return nil
}

func (s *Store) openConsumerFile() error {
	name := filepath.Join(s.dataDir, "consumer_runs.jsonl")
	f, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open consumer runs file: %w", err)
	}
	if s.consumerFile != nil {
		s.consumerFile.Close()
	}
	s.consumerFile = f
	return nil
}

func (s *Store) rotateIfNeeded() error {
	if s.rotationSizeMB <= 0 {
		return nil
	}
	info, err := s.currentFile.Stat()
	if err != nil {
		return nil
	}
	if info.Size() < int64(s.rotationSizeMB)*1024*1024 {
		return nil
	}
	s.currentFile.Close()
	src := filepath.Join(s.dataDir, "runs.jsonl")
	dst := filepath.Join(s.dataDir, fmt.Sprintf("runs-%d.jsonl", time.Now().Unix()))
	os.Rename(src, dst)
	return s.openFile()
}
