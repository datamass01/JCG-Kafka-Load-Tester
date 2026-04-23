package dashboard

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"kafka-agent/internal/config"
	"kafka-agent/internal/kafka"
	"kafka-agent/internal/metrics"
	"kafka-agent/internal/storage"
	"kafka-agent/web"
)

func NewServer(
	cfg *config.Config,
	admin *kafka.AdminClient,
	producer *kafka.Producer,
	collector *kafka.MetricsCollector,
	agg *metrics.Aggregator,
	store *storage.Store,
) *Server {
	hub := newHub()
	go hub.run()
	s := &Server{
		cfg:       cfg,
		admin:     admin,
		producer:  producer,
		collector: collector,
		agg:       agg,
		store:     store,
		hub:       hub,
	}
	producer.SetLogSink(s)
	return s
}

// Log satisfies kafka.LogSink — broadcasts a server-side log line to all WS clients.
func (s *Server) Log(level, msg string) {
	s.hub.sendJSON(map[string]any{
		"type":    "log",
		"level":   level,
		"message": msg,
		"ts":      time.Now().Format(time.RFC3339Nano),
	})
}

func (s *Server) SetReady(ready bool) {
	s.ready = ready
}

func (s *Server) Run(ctx context.Context) error {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware)

	r.Get("/health", s.handleHealth)
	r.Get("/ready", s.handleReady)

	r.Route("/api", func(r chi.Router) {
		r.Get("/config", s.handleGetConfig)
		r.Put("/config", s.handleUpdateConfig)
		r.Get("/kafka/brokers", s.handleBrokers)
		r.Get("/kafka/topics", s.handleTopics)
		r.Get("/kafka/metrics", s.handleKafkaMetrics)
		r.Post("/load/start", s.handleLoadStart)
		r.Post("/load/stop", s.handleLoadStop)
		r.Get("/load/status", s.handleLoadStatus)
		r.Get("/metrics/current", s.handleCurrentMetrics)
		r.Get("/metrics/history", s.handleHistory)
	})

	r.Get("/ws/metrics", s.handleWS)

	// Serve embedded web UI
	r.Handle("/*", http.FileServer(http.FS(web.Assets)))

	// Broadcast metrics every second
	go s.broadcastLoop(ctx)

	// Auto-save completed runs
	go s.runSaver(ctx)

	addr := fmt.Sprintf("%s:%d", s.cfg.Dashboard.Host, s.cfg.Dashboard.Port)
	log.Printf("dashboard listening on http://%s", addr)

	srv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) broadcastLoop(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("broadcastLoop panic: %v", r)
		}
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snap := s.agg.Snapshot()
			kafkaM := s.collector.Latest()
			status := "idle"
			elapsed := 0.0
			if s.producer.IsRunning() {
				status = "running"
				elapsed = time.Since(s.producer.StartedAt()).Seconds()
			}
			s.hub.sendJSON(map[string]any{
				"type":    "update",
				"status":  status,
				"elapsed": elapsed,
				"metrics": snap,
				"kafka":   kafkaM,
			})
		}
	}
}

func (s *Server) runSaver(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("runSaver panic: %v", r)
		}
	}()
	var wasRunning bool
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			isRunning := s.producer.IsRunning()
			if wasRunning && !isRunning {
				// Just stopped — save the run
				snap := s.agg.Snapshot()
				dur := time.Since(s.producer.StartedAt()).Seconds()
				avgMsgSec := 0.0
				if dur > 0 {
					avgMsgSec = float64(snap.TotalMessagesSent) / dur
				}
				avgMBSec := 0.0
				if dur > 0 {
					avgMBSec = float64(snap.TotalBytesSent) / dur / 1024 / 1024
				}
				record := storage.RunRecord{
					ID:           fmt.Sprintf("%d", s.producer.StartedAt().UnixNano()),
					StartedAt:    s.producer.StartedAt(),
					StoppedAt:    time.Now(),
					Topic:        s.cfg.LoadTest.Topic,
					Workers:      s.cfg.LoadTest.Workers,
					TargetMsgSec: s.cfg.LoadTest.TargetMsgPerSec,
					MsgSizeBytes: s.cfg.LoadTest.MessageSizeBytes,
					TotalSent:    snap.TotalMessagesSent,
					TotalErrors:  snap.TotalErrors,
					TotalBytes:   snap.TotalBytesSent,
					AvgMsgPerSec: avgMsgSec,
					AvgMBPerSec:  avgMBSec,
					AvgLatencyP99: snap.LatencyP99Ms,
				}
				if err := s.store.SaveRun(record); err != nil {
					log.Printf("save run: %v", err)
				}
			}
			wasRunning = isRunning
		}
	}
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
