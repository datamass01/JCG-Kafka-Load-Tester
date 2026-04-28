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

	inst := cfg.ActiveKafkaInstance()
	sc, err := kafka.NewSaramaConfig(inst.ToKafkaConfig())
	if err != nil {
		log.Fatalf("build sarama config: %v", err)
	}

	log.Printf("connecting to kafka instance %q: %v", inst.Name, inst.Brokers)
	admin, err := kafka.NewAdminClient(inst.Brokers, sc)
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
	producer := kafka.NewProducer(inst.Brokers, sc, &cfg.LoadTest, agg)

	consumerAgg := metrics.NewAggregator()
	consumer := kafka.NewConsumer(inst.Brokers, sc, &cfg.ConsumerTest, consumerAgg)

	collector := kafka.NewMetricsCollector(admin, 5*time.Second)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	collector.Start(ctx)

	srv := dashboard.NewServer(cfg, admin, producer, consumer, collector, agg, store)
	srv.SetReady(true)

	if err := srv.Run(ctx); err != nil {
		log.Printf("server: %v", err)
	}
}
