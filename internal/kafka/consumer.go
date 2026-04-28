package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"kafka-agent/internal/config"
	"kafka-agent/internal/metrics"
)

type Consumer struct {
	cfg           *config.ConsumerTestConfig
	brokers       []string
	saramaCfg     *sarama.Config
	agg           *metrics.Aggregator
	sink          LogSink
	onStop        func(startedAt, stoppedAt time.Time)
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	running       atomic.Bool
	startedAt     time.Time
	partitionLags sync.Map // int32 partition → int64 lag
}

func NewConsumer(brokers []string, sc *sarama.Config, cfg *config.ConsumerTestConfig, agg *metrics.Aggregator) *Consumer {
	return &Consumer{
		cfg:       cfg,
		brokers:   brokers,
		saramaCfg: sc,
		agg:       agg,
	}
}

func (c *Consumer) SetLogSink(sink LogSink) {
	c.sink = sink
}

func (c *Consumer) SetOnStop(fn func(startedAt, stoppedAt time.Time)) {
	c.onStop = fn
}

func (c *Consumer) logf(level, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("[consumer] %s: %s", level, msg)
	if c.sink != nil {
		c.sink.Log(level, msg)
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	if c.cfg.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if c.cfg.ConsumerGroup == "" {
		return fmt.Errorf("consumer_group is required")
	}
	if c.running.Swap(true) {
		return fmt.Errorf("consumer already running")
	}

	c.startedAt = time.Now()
	c.agg.Reset()
	c.partitionLags.Range(func(k, _ any) bool { c.partitionLags.Delete(k); return true })

	consumerCfg := *c.saramaCfg
	consumerCfg.Consumer.Return.Errors = true
	consumerCfg.Consumer.Offsets.AutoCommit.Enable = true
	consumerCfg.Consumer.Offsets.AutoCommit.Interval = time.Second
	if c.cfg.OffsetReset == "earliest" {
		consumerCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		consumerCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	c.logf("info", "creating consumer group (brokers=%v topic=%s group=%s)", c.brokers, c.cfg.Topic, c.cfg.ConsumerGroup)

	group, err := sarama.NewConsumerGroup(c.brokers, c.cfg.ConsumerGroup, &consumerCfg)
	if err != nil {
		c.running.Store(false)
		c.logf("error", "create consumer group: %v", err)
		return fmt.Errorf("create consumer group: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	if c.cfg.DurationSeconds > 0 {
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(c.cfg.DurationSeconds)*time.Second)
	}
	c.cancel = cancel

	c.logf("success", "consumer group ready — consuming topic %q as group %q", c.cfg.Topic, c.cfg.ConsumerGroup)

	handler := &consumerGroupHandler{consumer: c}

	// Drain group errors
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for e := range group.Errors() {
			c.agg.RecordError(e)
		}
	}()

	// Main consume loop — Sarama calls ConsumeClaim for each partition
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				msg := fmt.Sprintf("panic in consume loop: %v", r)
				log.Printf("[consumer] PANIC: %s", msg)
				if c.sink != nil {
					c.sink.Log("error", msg)
				}
			}
		}()
		for {
			if err := group.Consume(runCtx, []string{c.cfg.Topic}, handler); err != nil {
				if runCtx.Err() != nil {
					return
				}
				c.logf("error", "consume session error: %v", err)
			}
			if runCtx.Err() != nil {
				return
			}
		}
	}()

	go func() {
		<-runCtx.Done()
		c.logf("info", "stop signal received — closing consumer group")
		_ = group.Close()
		c.wg.Wait()
		c.agg.MarkStopped()
		c.running.Store(false)
		c.logf("info", "consumer fully stopped")
		if c.onStop != nil {
			c.onStop(c.startedAt, time.Now())
		}
	}()

	return nil
}

func (c *Consumer) Stop() {
	c.running.Store(false)
	if c.cancel != nil {
		c.cancel()
	}
}

func (c *Consumer) IsRunning() bool {
	return c.running.Load()
}

func (c *Consumer) StartedAt() time.Time {
	return c.startedAt
}

func (c *Consumer) totalLag() int64 {
	var sum int64
	c.partitionLags.Range(func(_, v any) bool {
		sum += v.(int64)
		return true
	})
	return sum
}

func (c *Consumer) SetBrokers(brokers []string, sc *sarama.Config) error {
	if c.running.Load() {
		return fmt.Errorf("cannot change brokers while consumer is running")
	}
	c.brokers = brokers
	c.saramaCfg = sc
	return nil
}

func (c *Consumer) Snapshot() metrics.ConsumerSnapshot {
	snap := c.agg.Snapshot()
	return metrics.ConsumerSnapshot{
		TotalMessagesConsumed: snap.TotalMessagesSent,
		TotalErrors:           snap.TotalErrors,
		TotalBytesConsumed:    snap.TotalBytesSent,
		MsgPerSec:             snap.MsgPerSec,
		MBPerSec:              snap.MBPerSec,
		ErrorRatePct:          snap.ErrorRatePct,
		LatencyP50Ms:          snap.LatencyP50Ms,
		LatencyP95Ms:          snap.LatencyP95Ms,
		LatencyP99Ms:          snap.LatencyP99Ms,
		LagMessages:           c.totalLag(),
		LastError:             snap.LastError,
		Timestamp:             snap.Timestamp,
	}
}

type consumerGroupHandler struct {
	consumer *Consumer
}

func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c := h.consumer
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			// Track lag per partition; Snapshot sums across all assigned partitions
			lag := claim.HighWaterMarkOffset() - msg.Offset - 1
			if lag < 0 {
				lag = 0
			}
			c.partitionLags.Store(claim.Partition(), lag)

			// E2E latency: producer embeds send timestamp as first 8 bytes (big-endian int64 nanoseconds)
			var latency time.Duration
			if len(msg.Value) >= 8 {
				ns := int64(binary.BigEndian.Uint64(msg.Value[:8]))
				sentAt := time.Unix(0, ns)
				if !sentAt.IsZero() && sentAt.Before(time.Now()) {
					d := time.Since(sentAt)
					// Sanity check: discard if > 10 minutes (likely random bytes, not our timestamp)
					if d < 10*time.Minute {
						latency = d
					}
				}
			}
			c.agg.RecordSuccess(latency, len(msg.Value))
			session.MarkMessage(msg, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
