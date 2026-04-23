package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"kafka-agent/internal/config"
	"kafka-agent/internal/kafka"
	"kafka-agent/internal/metrics"
	"kafka-agent/internal/storage"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Server struct {
	cfg       *config.Config
	admin     *kafka.AdminClient
	producer  *kafka.Producer
	collector *kafka.MetricsCollector
	agg       *metrics.Aggregator
	store     *storage.Store
	hub       *wsHub
	ready     bool
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.ready {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	} else {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.cfg)
}

func (s *Server) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	if s.producer.IsRunning() {
		http.Error(w, "stop load test before changing config", http.StatusConflict)
		return
	}
	var patch config.LoadTestConfig
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.cfg.LoadTest = patch
	writeJSON(w, s.cfg.LoadTest)
}

func (s *Server) handleBrokers(w http.ResponseWriter, r *http.Request) {
	m := s.collector.Latest()
	writeJSON(w, m.Brokers)
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	m := s.collector.Latest()
	writeJSON(w, m.Topics)
}

func (s *Server) handleKafkaMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.collector.Latest())
}

func (s *Server) handleLoadStart(w http.ResponseWriter, r *http.Request) {
	s.Log("info", fmt.Sprintf("/api/load/start called (topic=%s workers=%d rate=%d)",
		s.cfg.LoadTest.Topic, s.cfg.LoadTest.Workers, s.cfg.LoadTest.TargetMsgPerSec))
	if err := s.producer.Start(context.Background()); err != nil {
		s.Log("error", fmt.Sprintf("start rejected: %v", err))
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, map[string]string{"status": "started"})
}

func (s *Server) handleLoadStop(w http.ResponseWriter, r *http.Request) {
	s.Log("info", "/api/load/stop called")
	s.producer.Stop()
	writeJSON(w, map[string]string{"status": "stopped"})
}

func (s *Server) handleLoadStatus(w http.ResponseWriter, r *http.Request) {
	snap := s.agg.Snapshot()
	status := "idle"
	elapsed := 0.0
	if s.producer.IsRunning() {
		status = "running"
		elapsed = time.Since(s.producer.StartedAt()).Seconds()
	}
	writeJSON(w, map[string]any{
		"status":  status,
		"elapsed": elapsed,
		"config":  s.cfg.LoadTest,
		"metrics": snap,
	})
}

func (s *Server) handleCurrentMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.agg.Snapshot())
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	since := time.Now().Add(-time.Duration(s.cfg.Storage.MetricsRetentionHours) * time.Hour)
	runs, err := s.store.ListRuns(since)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, runs)
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	client := &wsClient{hub: s.hub, conn: conn, send: make(chan []byte, 32)}
	s.hub.register <- client
	go client.writePump()
	go client.readPump()
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, fmt.Sprintf("encode response: %v", err), http.StatusInternalServerError)
	}
}
