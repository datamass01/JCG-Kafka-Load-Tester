package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafka-agent/internal/config"
	"kafka-agent/internal/dashboard"
	"kafka-agent/internal/kafka"
	"kafka-agent/internal/metrics"
	"kafka-agent/internal/storage"
)

func main() {
	cfgPath := flag.String("config", "/etc/kafka-agent/config.yaml", "path to config file")
	flag.Parse()

	// Fall back to local config for development
	if _, err := os.Stat(*cfgPath); os.IsNotExist(err) {
		*cfgPath = "config/config.yaml"
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	sc, err := kafka.NewSaramaConfig(&cfg.Kafka)
	if err != nil {
		log.Fatalf("build sarama config: %v", err)
	}

	log.Printf("connecting to kafka: %v", cfg.Kafka.Brokers)
	admin, err := kafka.NewAdminClient(cfg.Kafka.Brokers, sc)
	if err != nil {
		log.Fatalf("kafka admin client: %v", err)
	}
	defer admin.Close()

	if err := admin.EnsureTopic(&cfg.LoadTest); err != nil {
		log.Printf("warn: ensure topic: %v", err)
	}

	store, err := storage.NewStore(cfg.Storage.DataDir, cfg.Storage.RotationSizeMB)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer store.Close()

	agg := metrics.NewAggregator()
	producer := kafka.NewProducer(cfg.Kafka.Brokers, sc, &cfg.LoadTest, agg)
	collector := kafka.NewMetricsCollector(admin, 5*time.Second)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	collector.Start(ctx)

	srv := dashboard.NewServer(cfg, admin, producer, collector, agg, store)
	srv.SetReady(true)

	if err := srv.Run(ctx); err != nil {
		log.Printf("server: %v", err)
	}
}
