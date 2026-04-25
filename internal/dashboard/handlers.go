package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
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
	cfg          *config.Config
	admin        *kafka.AdminClient
	producer     *kafka.Producer
	collector    *kafka.MetricsCollector
	agg          *metrics.Aggregator
	store        *storage.Store
	hub          *wsHub
	ready        bool
	instanceMu   sync.RWMutex
	rootCtx      context.Context
	prevInstance string // instance active before the last ad-hoc connect
}

func (s *Server) activeCollector() *kafka.MetricsCollector {
	s.instanceMu.RLock()
	defer s.instanceMu.RUnlock()
	return s.collector
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
	m := s.activeCollector().Latest()
	writeJSON(w, m.Brokers)
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	m := s.activeCollector().Latest()
	writeJSON(w, m.Topics)
}

func (s *Server) handleKafkaMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.activeCollector().Latest())
}


// handleConnectBrokers accepts a comma-separated broker string, registers it as an
// ad-hoc instance if not already known, and makes it the active instance.
func (s *Server) handleConnectBrokers(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Brokers string `json:"brokers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	brokers := splitBrokers(body.Brokers)
	if len(brokers) == 0 {
		http.Error(w, "brokers required", http.StatusBadRequest)
		return
	}
	name := strings.Join(brokers, ",")

	s.instanceMu.Lock()
	if s.cfg.FindInstance(name) == nil {
		version := s.cfg.Kafka.Version
		if active := s.cfg.ActiveKafkaInstance(); active != nil {
			version = active.Version
		}
		s.cfg.KafkaInstances = append(s.cfg.KafkaInstances, config.KafkaInstanceConfig{
			Name:    name,
			Brokers: brokers,
			Version: version,
			AdHoc:   true,
		})
	}
	// Remember the pre-connect instance the first time we go ad-hoc.
	if s.prevInstance == "" {
		if current := s.cfg.ActiveKafkaInstance(); current != nil && !current.AdHoc {
			s.prevInstance = current.Name
		}
	}
	s.instanceMu.Unlock()

	if err := s.switchInstance(name); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, map[string]string{"active_instance": name})
}

// handleDisconnect reverts to the instance that was active before the last ad-hoc connect.
func (s *Server) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	s.instanceMu.Lock()
	target := s.prevInstance
	if target == "" && len(s.cfg.KafkaInstances) > 0 {
		target = s.cfg.KafkaInstances[0].Name
	}
	s.instanceMu.Unlock()

	if target == "" {
		http.Error(w, "no instance to revert to", http.StatusConflict)
		return
	}
	if err := s.switchInstance(target); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	s.instanceMu.Lock()
	s.prevInstance = ""
	s.instanceMu.Unlock()

	writeJSON(w, map[string]string{"active_instance": target})
}

func splitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
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
