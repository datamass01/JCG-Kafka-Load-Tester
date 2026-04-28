package kafka

import (
	"context"
	"encoding/binary"
	"errors"
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
	cfg          *config.LoadTestConfig
	brokers      []string
	saramaCfg    *sarama.Config
	agg          *metrics.Aggregator
	sink         LogSink
	onStop       func(startedAt, stoppedAt time.Time)
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	running      atomic.Bool
	startedAt    time.Time
	asyncProd    sarama.AsyncProducer
	client       sarama.Client
	lastLeaderLog atomic.Int64 // unix-nano timestamp of last "leader not available" log
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

// SetOnStop registers a callback fired once after the producer has fully drained.
func (p *Producer) SetOnStop(fn func(startedAt, stoppedAt time.Time)) {
	p.onStop = fn
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

	// Build the producer on top of an explicit client so we can force a
	// metadata refresh before sending. This ensures a freshly started test
	// targets the current leader, not stale metadata cached from a previous
	// test that may have ended during a failover.
	client, err := sarama.NewClient(p.brokers, &producerCfg)
	if err != nil {
		p.running.Store(false)
		p.logf("error", "create kafka client: %v", err)
		return fmt.Errorf("create client: %w", err)
	}
	if err := client.RefreshMetadata(p.cfg.Topic); err != nil {
		p.logf("warn", "metadata refresh on start: %v (continuing)", err)
	}
	asyncProd, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		client.Close()
		p.running.Store(false)
		p.logf("error", "create producer: %v", err)
		return fmt.Errorf("create producer: %w", err)
	}
	p.client = client
	p.asyncProd = asyncProd
	p.lastLeaderLog.Store(0)
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
		// AsyncClose signals the producer to flush without blocking. The existing
		// collector goroutines drain Successes/Errors until the channels close.
		asyncProd.AsyncClose()
		p.wg.Wait()
		if p.client != nil {
			p.client.Close()
			p.client = nil
		}
		p.asyncProd = nil
		p.agg.MarkStopped()
		p.running.Store(false) // covers duration-based stop where Stop() was never called
		p.logf("info", "load test fully stopped")
		if p.onStop != nil {
			p.onStop(p.startedAt, time.Now())
		}
	}()

	return nil
}

func (p *Producer) Stop() {
	// Mark stopped immediately so the UI reflects the change without waiting
	// for the producer to finish flushing (which blocks when a broker is down).
	p.running.Store(false)
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

// SetBrokers updates the target broker list and sarama config.
// Returns an error if a load test is currently running.
func (p *Producer) SetBrokers(brokers []string, sc *sarama.Config) error {
	if p.running.Load() {
		return fmt.Errorf("cannot change brokers while load test is running")
	}
	p.brokers = brokers
	p.saramaCfg = sc
	return nil
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
			now := time.Now()
			msg := p.buildMessage(now)
			msg.Metadata = now
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
		if isLeaderNotAvailable(e.Err) {
			// Sarama already exhausted all retries — election is taking longer
			// than the retry window. Rate-limit the user-facing log to one
			// message every 5 seconds so the event log stays readable, and
			// kick off a metadata refresh so the new leader is picked up the
			// moment the election completes.
			now := time.Now().UnixNano()
			last := p.lastLeaderLog.Load()
			if now-last > int64(5*time.Second) {
				if p.lastLeaderLog.CompareAndSwap(last, now) {
					p.logf("warn", "leader not available — Kafka leader election in progress; will recover automatically")
					if p.client != nil {
						go func() {
							defer recoverPanic(p, "metadata refresh")
							_ = p.client.RefreshMetadata()
						}()
					}
				}
			}
		}
		p.agg.RecordError(e.Err)
	}
}

func isLeaderNotAvailable(err error) bool {
	var kErr sarama.KError
	if errors.As(err, &kErr) {
		return kErr == sarama.ErrLeaderNotAvailable ||
			kErr == sarama.ErrNotLeaderForPartition ||
			kErr == sarama.ErrBrokerNotAvailable ||
			kErr == sarama.ErrNetworkException
	}
	return false
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

func (p *Producer) buildMessage(now time.Time) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: p.cfg.Topic,
		Key:   sarama.ByteEncoder(p.generateKey()),
		Value: sarama.ByteEncoder(p.generateValue(now)),
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

func (p *Producer) generateValue(now time.Time) []byte {
	size := p.cfg.MessageSizeBytes
	if size < 8 {
		size = 8 // ensure room for the embedded timestamp
	}
	b := make([]byte, size)
	// First 8 bytes: send timestamp as big-endian int64 nanoseconds (read by consumer for E2E latency)
	binary.BigEndian.PutUint64(b[:8], uint64(now.UnixNano()))
	switch p.cfg.ValueStrategy {
	case "random":
		rand.Read(b[8:])
	default:
		for i := 8; i < len(b); i++ {
			b[i] = byte('A' + (i % 26))
		}
	}
	return b
}
