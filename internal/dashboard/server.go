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
	consumer *kafka.Consumer,
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
		consumer:  consumer,
		collector: collector,
		agg:       agg,
		store:     store,
		hub:       hub,
	}
	producer.SetLogSink(s)
	consumer.SetLogSink(s)
	producer.SetOnStop(s.saveRun)
	consumer.SetOnStop(s.saveConsumerRun)
	return s
}

// saveRun persists a completed producer run and notifies clients. Called by the
// producer once it has fully drained, so no polling is required.
func (s *Server) saveRun(startedAt, stoppedAt time.Time) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("saveRun panic: %v", r)
		}
	}()
	snap := s.agg.Snapshot()
	dur := stoppedAt.Sub(startedAt).Seconds()
	avgMsgSec := 0.0
	avgMBSec := 0.0
	if dur > 0 {
		avgMsgSec = float64(snap.TotalMessagesSent) / dur
		avgMBSec = float64(snap.TotalBytesSent) / dur / 1024 / 1024
	}
	record := storage.RunRecord{
		ID:            fmt.Sprintf("%d", startedAt.UnixNano()),
		StartedAt:     startedAt,
		StoppedAt:     stoppedAt,
		Topic:         s.cfg.LoadTest.Topic,
		Workers:       s.cfg.LoadTest.Workers,
		TargetMsgSec:  s.cfg.LoadTest.TargetMsgPerSec,
		MsgSizeBytes:  s.cfg.LoadTest.MessageSizeBytes,
		TotalSent:     snap.TotalMessagesSent,
		TotalErrors:   snap.TotalErrors,
		TotalBytes:    snap.TotalBytesSent,
		AvgMsgPerSec:  avgMsgSec,
		AvgMBPerSec:   avgMBSec,
		AvgLatencyP99: snap.LatencyP99Ms,
	}
	if err := s.store.SaveRun(record); err != nil {
		log.Printf("save run: %v", err)
		return
	}
	s.hub.sendJSON(map[string]any{"type": "run_completed"})
}

func (s *Server) saveConsumerRun(startedAt, stoppedAt time.Time) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("saveConsumerRun panic: %v", r)
		}
	}()
	snap := s.consumer.Snapshot()
	dur := stoppedAt.Sub(startedAt).Seconds()
	avgMsgSec := 0.0
	avgMBSec := 0.0
	if dur > 0 {
		avgMsgSec = float64(snap.TotalMessagesConsumed) / dur
		avgMBSec = float64(snap.TotalBytesConsumed) / dur / 1024 / 1024
	}
	record := storage.ConsumerRunRecord{
		ID:            fmt.Sprintf("%d", startedAt.UnixNano()),
		StartedAt:     startedAt,
		StoppedAt:     stoppedAt,
		Topic:         s.cfg.ConsumerTest.Topic,
		ConsumerGroup: s.cfg.ConsumerTest.ConsumerGroup,
		OffsetReset:   s.cfg.ConsumerTest.OffsetReset,
		TotalConsumed: snap.TotalMessagesConsumed,
		TotalErrors:   snap.TotalErrors,
		TotalBytes:    snap.TotalBytesConsumed,
		AvgMsgPerSec:  avgMsgSec,
		AvgMBPerSec:   avgMBSec,
		AvgLatencyP99: snap.LatencyP99Ms,
	}
	if err := s.store.SaveConsumerRun(record); err != nil {
		log.Printf("save consumer run: %v", err)
		return
	}
	s.hub.sendJSON(map[string]any{"type": "consumer_run_completed"})
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
	s.instanceMu.Lock()
	s.rootCtx = ctx
	s.instanceMu.Unlock()

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
		r.Post("/kafka/connect", s.handleConnectBrokers)
		r.Post("/kafka/disconnect", s.handleDisconnect)
		r.Post("/load/start", s.handleLoadStart)
		r.Post("/load/stop", s.handleLoadStop)
		r.Get("/load/status", s.handleLoadStatus)
		r.Post("/load/consumer/start", s.handleConsumerStart)
		r.Post("/load/consumer/stop", s.handleConsumerStop)
		r.Get("/load/consumer/status", s.handleConsumerStatus)
		r.Put("/consumer/config", s.handleUpdateConsumerConfig)
		r.Get("/metrics/current", s.handleCurrentMetrics)
		r.Get("/metrics/history", s.handleHistory)
		r.Get("/metrics/consumer/history", s.handleConsumerHistory)
	})

	r.Get("/ws/metrics", s.handleWS)

	// Serve embedded web UI
	r.Handle("/*", http.FileServer(http.FS(web.Assets)))

	// Broadcast metrics every second
	go s.broadcastLoop(ctx)

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

// switchInstance reconnects to the named Kafka instance. Rejected if a load test is running.
func (s *Server) switchInstance(name string) error {
	s.instanceMu.Lock()
	defer s.instanceMu.Unlock()

	if s.producer.IsRunning() || s.consumer.IsRunning() {
		return fmt.Errorf("stop load test before switching instance")
	}

	inst := s.cfg.FindInstance(name)
	if inst == nil {
		return fmt.Errorf("instance %q not found", name)
	}

	sc, err := kafka.NewSaramaConfig(inst.ToKafkaConfig())
	if err != nil {
		return fmt.Errorf("build sarama config: %w", err)
	}

	newAdmin, err := kafka.NewAdminClient(inst.Brokers, sc)
	if err != nil {
		return fmt.Errorf("connect to %q (%v): %w", name, inst.Brokers, err)
	}

	// Stop old collector goroutine and close old admin.
	s.collector.Stop()
	s.admin.Close()

	s.admin = newAdmin
	s.collector = kafka.NewMetricsCollector(newAdmin, 5*time.Second)
	s.collector.Start(s.rootCtx)

	if err := s.producer.SetBrokers(inst.Brokers, sc); err != nil {
		return err
	}
	if err := s.consumer.SetBrokers(inst.Brokers, sc); err != nil {
		return err
	}

	s.cfg.ActiveInstance = name

	// Best-effort topic creation on the new instance.
	if err := newAdmin.EnsureTopic(&s.cfg.LoadTest); err != nil {
		log.Printf("warn: ensure topic on %q: %v", name, err)
	}

	s.Log("info", fmt.Sprintf("switched to Kafka instance %q (%v)", name, inst.Brokers))
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
			consumerSnap := s.consumer.Snapshot()
			kafkaM := s.activeCollector().Latest()
			status := "idle"
			elapsed := 0.0
			if s.producer.IsRunning() {
				status = "running"
				elapsed = time.Since(s.producer.StartedAt()).Seconds()
			}
			consumerStatus := "idle"
			consumerElapsed := 0.0
			if s.consumer.IsRunning() {
				consumerStatus = "running"
				consumerElapsed = time.Since(s.consumer.StartedAt()).Seconds()
			}
			s.instanceMu.RLock()
			activeInst := s.cfg.ActiveInstance
			s.instanceMu.RUnlock()
			s.hub.sendJSON(map[string]any{
				"type":             "update",
				"status":           status,
				"elapsed":          elapsed,
				"metrics":          snap,
				"consumer_status":  consumerStatus,
				"consumer_elapsed": consumerElapsed,
				"consumer_metrics": consumerSnap,
				"kafka":            kafkaM,
				"active_instance":  activeInst,
			})
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
