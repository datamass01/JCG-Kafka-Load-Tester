package kafka

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"kafka-agent/internal/config"
)

type TopicInfo struct {
	Name              string          `json:"name"`
	Partitions        int             `json:"partitions"`
	ReplicationFactor int             `json:"replication_factor"`
	OffsetsByPartition map[int32]int64 `json:"offsets_by_partition"`
}

type BrokerInfo struct {
	ID      int32  `json:"id"`
	Host    string `json:"host"`
	Port    int32  `json:"port"`
	Connected bool  `json:"connected"`
}

type AdminClient struct {
	admin   sarama.ClusterAdmin
	client  sarama.Client
	brokers []string
}

func NewAdminClient(brokers []string, sc *sarama.Config) (*AdminClient, error) {
	client, err := sarama.NewClient(brokers, sc)
	if err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("new admin: %w", err)
	}
	return &AdminClient{admin: admin, client: client, brokers: brokers}, nil
}

func (a *AdminClient) Close() {
	a.admin.Close()
	a.client.Close()
}

func (a *AdminClient) EnsureTopic(cfg *config.LoadTestConfig) error {
	topics, err := a.admin.ListTopics()
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}
	if _, exists := topics[cfg.Topic]; exists {
		return nil
	}
	detail := &sarama.TopicDetail{
		NumPartitions:     int32(cfg.Partitions),
		ReplicationFactor: int16(cfg.ReplicationFactor),
	}
	if err := a.admin.CreateTopic(cfg.Topic, detail, false); err != nil {
		return fmt.Errorf("create topic %s: %w", cfg.Topic, err)
	}
	return nil
}

func (a *AdminClient) ListTopics() ([]TopicInfo, error) {
	topics, err := a.admin.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}

	result := make([]TopicInfo, 0, len(topics))
	for name, detail := range topics {
		if len(name) > 0 && name[0] == '_' {
			continue
		}
		info := TopicInfo{
			Name:              name,
			Partitions:        int(detail.NumPartitions),
			ReplicationFactor: int(detail.ReplicationFactor),
			OffsetsByPartition: make(map[int32]int64),
		}
		for p := int32(0); p < detail.NumPartitions; p++ {
			offset, err := a.client.GetOffset(name, p, sarama.OffsetNewest)
			if err == nil {
				info.OffsetsByPartition[p] = offset
			}
		}
		result = append(result, info)
	}
	return result, nil
}

func (a *AdminClient) ListBrokers() ([]BrokerInfo, error) {
	brokers := a.client.Brokers()
	result := make([]BrokerInfo, 0, len(brokers))
	for _, b := range brokers {
		connected := false
		if err := b.Open(a.client.Config()); err == nil || err == sarama.ErrAlreadyConnected {
			connected = true
		}
		result = append(result, BrokerInfo{
			ID:        b.ID(),
			Host:      b.Addr(),
			Connected: connected,
		})
	}
	return result, nil
}

func (a *AdminClient) IsReady() bool {
	brokers := a.client.Brokers()
	for _, b := range brokers {
		if err := b.Open(a.client.Config()); err == nil || err == sarama.ErrAlreadyConnected {
			connected, _ := b.Connected()
			if connected {
				return true
			}
		}
	}
	// Try a metadata refresh as a connectivity check
	ctx := make(chan struct{})
	go func() {
		a.client.RefreshMetadata()
		close(ctx)
	}()
	select {
	case <-ctx:
		return a.client.Closed() == false
	case <-time.After(2 * time.Second):
		return false
	}
}
