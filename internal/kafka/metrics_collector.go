package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type KafkaMetrics struct {
	Brokers         []BrokerInfo  `json:"brokers"`
	Topics          []TopicInfo   `json:"topics"`
	CollectedAt     time.Time     `json:"collected_at"`
	CollectionError string        `json:"collection_error,omitempty"`
}

type MetricsCollector struct {
	admin    *AdminClient
	interval time.Duration
	mu       sync.RWMutex
	latest   *KafkaMetrics
	stopFn   context.CancelFunc
}

func NewMetricsCollector(admin *AdminClient, interval time.Duration) *MetricsCollector {
	return &MetricsCollector{
		admin:    admin,
		interval: interval,
		latest:   &KafkaMetrics{},
	}
}

func (mc *MetricsCollector) Start(ctx context.Context) {
	innerCtx, cancel := context.WithCancel(ctx)
	mc.stopFn = cancel
	mc.collect()
	ticker := time.NewTicker(mc.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-innerCtx.Done():
				return
			case <-ticker.C:
				mc.collect()
			}
		}
	}()
}

// Stop cancels the polling goroutine started by Start.
func (mc *MetricsCollector) Stop() {
	if mc.stopFn != nil {
		mc.stopFn()
	}
}

func (mc *MetricsCollector) Latest() *KafkaMetrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.latest
}

func (mc *MetricsCollector) collect() {
	m := &KafkaMetrics{CollectedAt: time.Now()}

	brokers, err := mc.admin.ListBrokers()
	if err != nil {
		// Refresh metadata so the client re-discovers live brokers, then retry once.
		mc.admin.Refresh()
		brokers, err = mc.admin.ListBrokers()
	}
	if err != nil {
		m.CollectionError = err.Error()
	} else {
		m.Brokers = brokers
	}

	topics, err := mc.admin.ListTopics()
	if err != nil {
		if m.CollectionError == "" {
			m.CollectionError = err.Error()
		}
	} else {
		m.Topics = topics
	}

	mc.mu.Lock()
	mc.latest = m
	mc.mu.Unlock()
}

// ConsumerGroupLag returns the lag for a consumer group on a topic.
func ConsumerGroupLag(client sarama.Client, group, topic string) (map[int32]int64, error) {
	offsetManager, err := sarama.NewOffsetManagerFromClient(group, client)
	if err != nil {
		return nil, err
	}
	defer offsetManager.Close()

	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, err
	}

	lag := make(map[int32]int64, len(partitions))
	for _, p := range partitions {
		newest, err := client.GetOffset(topic, p, sarama.OffsetNewest)
		if err != nil {
			continue
		}
		pm, err := offsetManager.ManagePartition(topic, p)
		if err != nil {
			continue
		}
		committed, _ := pm.NextOffset()
		pm.Close()
		if committed < 0 {
			committed = 0
		}
		lag[p] = newest - committed
	}
	return lag, nil
}
