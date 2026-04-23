package kafka

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"kafka-agent/internal/config"
	"kafka-agent/internal/metrics"
)

type LogSink interface {
	Log(level, msg string)
}

type Producer struct {
	cfg       *config.LoadTestConfig
	brokers   []string
	saramaCfg *sarama.Config
	agg       *metrics.Aggregator
	sink      LogSink
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	running   atomic.Bool
	startedAt time.Time
}

func NewProducer(brokers []string, sc *sarama.Config, cfg *config.LoadTestConfig, agg *metrics.Aggregator) *Producer {
	return &Producer{
		cfg:       cfg,
		brokers:   brokers,
		saramaCfg: sc,
		agg:       agg,
	}
}

func (p *Producer) SetLogSink(sink LogSink) {
	p.sink = sink
}

func (p *Producer) logf(level, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("[producer] %s: %s", level, msg)
	if p.sink != nil {
		p.sink.Log(level, msg)
	}
}

func (p *Producer) Start(ctx context.Context) error {
	if p.cfg.Workers <= 0 {
		return fmt.Errorf("workers must be > 0 (got %d)", p.cfg.Workers)
	}
	if p.cfg.TargetMsgPerSec <= 0 {
		return fmt.Errorf("target_msg_per_sec must be > 0 (got %d)", p.cfg.TargetMsgPerSec)
	}
	if p.cfg.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if p.running.Swap(true) {
		return fmt.Errorf("load test already running")
	}

	p.startedAt = time.Now()
	p.agg.Reset()

	// Build a dedicated producer config so we don't mutate the shared admin config
	producerCfg := *p.saramaCfg
	producerCfg.Producer.Return.Successes = true
	producerCfg.Producer.Return.Errors = true
	producerCfg.Producer.RequiredAcks = sarama.WaitForLocal
	producerCfg.Producer.Compression = sarama.CompressionNone
	producerCfg.Producer.Flush.Frequency = 10 * time.Millisecond
	producerCfg.Producer.Flush.Messages = 100

	p.logf("info", "creating async producer (brokers=%v topic=%s)", p.brokers, p.cfg.Topic)
	asyncProd, err := sarama.NewAsyncProducer(p.brokers, &producerCfg)
	if err != nil {
		p.running.Store(false)
		p.logf("error", "create producer: %v", err)
		return fmt.Errorf("create producer: %w", err)
	}
	p.logf("success", "producer connected — launching %d workers at %d msg/s target", p.cfg.Workers, p.cfg.TargetMsgPerSec)

	runCtx, cancel := context.WithCancel(ctx)
	if p.cfg.DurationSeconds > 0 {
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(p.cfg.DurationSeconds)*time.Second)
	}
	p.cancel = cancel

	// Success / error collectors (one goroutine each, panic-safe)
	p.wg.Add(2)
	go p.collectSuccess(asyncProd)
	go p.collectErrors(asyncProd)

	// Rate-limited senders
	ratePerWorker := float64(p.cfg.TargetMsgPerSec) / float64(p.cfg.Workers)
	interval := time.Duration(float64(time.Second) / ratePerWorker)
	if interval <= 0 {
		interval = time.Microsecond
	}

	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.runWorker(runCtx, asyncProd, interval)
	}

	go func() {
		defer recoverPanic(p, "shutdown goroutine")
		<-runCtx.Done()
		p.logf("info", "stop signal received — draining producer")
		if err := asyncProd.Close(); err != nil {
			p.logf("error", "producer close: %v", err)
		}
		p.wg.Wait()
		p.running.Store(false)
		p.agg.MarkStopped()
		p.logf("info", "load test fully stopped")
	}()

	return nil
}

func (p *Producer) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

func (p *Producer) IsRunning() bool {
	return p.running.Load()
}

func (p *Producer) StartedAt() time.Time {
	return p.startedAt
}

func (p *Producer) runWorker(ctx context.Context, asyncProd sarama.AsyncProducer, interval time.Duration) {
	defer p.wg.Done()
	defer recoverPanic(p, "worker")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	input := asyncProd.Input()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msg := p.buildMessage()
			msg.Metadata = time.Now()
			select {
			case input <- msg:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (p *Producer) collectSuccess(asyncProd sarama.AsyncProducer) {
	defer p.wg.Done()
	defer recoverPanic(p, "success collector")
	for msg := range asyncProd.Successes() {
		sentAt, ok := msg.Metadata.(time.Time)
		if !ok {
			continue
		}
		bytes := 0
		if be, ok := msg.Value.(sarama.ByteEncoder); ok {
			bytes = len(be)
		}
		p.agg.RecordSuccess(time.Since(sentAt), bytes)
	}
}

func (p *Producer) collectErrors(asyncProd sarama.AsyncProducer) {
	defer p.wg.Done()
	defer recoverPanic(p, "error collector")
	for e := range asyncProd.Errors() {
		p.agg.RecordError(e.Err)
	}
}

func recoverPanic(p *Producer, where string) {
	if r := recover(); r != nil {
		msg := fmt.Sprintf("panic in %s: %v", where, r)
		log.Printf("[producer] PANIC: %s", msg)
		if p.sink != nil {
			p.sink.Log("error", msg)
		}
		if p.agg != nil {
			p.agg.RecordError(fmt.Errorf("%s", msg))
		}
	}
}

func (p *Producer) buildMessage() *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: p.cfg.Topic,
		Key:   sarama.ByteEncoder(p.generateKey()),
		Value: sarama.ByteEncoder(p.generateValue()),
	}
}

func (p *Producer) generateKey() []byte {
	switch p.cfg.KeyStrategy {
	case "sequential":
		return []byte(fmt.Sprintf("key-%d", time.Now().UnixNano()))
	case "fixed":
		return []byte("load-test-key")
	default:
		b := make([]byte, 16)
		rand.Read(b)
		return []byte(fmt.Sprintf("%x", b))
	}
}

func (p *Producer) generateValue() []byte {
	size := p.cfg.MessageSizeBytes
	if size <= 0 {
		size = 1024
	}
	switch p.cfg.ValueStrategy {
	case "random":
		b := make([]byte, size)
		rand.Read(b)
		return b
	default:
		b := make([]byte, size)
		for i := range b {
			b[i] = byte('A' + (i % 26))
		}
		return b
	}
}
