package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka     KafkaConfig     `yaml:"kafka"     json:"kafka"`
	LoadTest  LoadTestConfig  `yaml:"load_test" json:"load_test"`
	Dashboard DashboardConfig `yaml:"dashboard" json:"dashboard"`
	Storage   StorageConfig   `yaml:"storage"   json:"storage"`
}

type KafkaConfig struct {
	Brokers []string   `yaml:"brokers" json:"brokers"`
	SASL    SASLConfig `yaml:"sasl"    json:"sasl"`
	TLS     TLSConfig  `yaml:"tls"     json:"tls"`
	Version string     `yaml:"version" json:"version"`
}

type SASLConfig struct {
	Enabled   bool   `yaml:"enabled"   json:"enabled"`
	Mechanism string `yaml:"mechanism" json:"mechanism"`
	Username  string `yaml:"username"  json:"username"`
	Password  string `yaml:"password"  json:"password"`
}

type TLSConfig struct {
	Enabled    bool   `yaml:"enabled"     json:"enabled"`
	SkipVerify bool   `yaml:"skip_verify" json:"skip_verify"`
	CACert     string `yaml:"ca_cert"     json:"ca_cert"`
	ClientCert string `yaml:"client_cert" json:"client_cert"`
	ClientKey  string `yaml:"client_key"  json:"client_key"`
}

type LoadTestConfig struct {
	Topic             string `yaml:"topic"              json:"topic"`
	Partitions        int    `yaml:"partitions"         json:"partitions"`
	ReplicationFactor int    `yaml:"replication_factor" json:"replication_factor"`
	Workers           int    `yaml:"workers"            json:"workers"`
	TargetMsgPerSec   int    `yaml:"target_msg_per_sec" json:"target_msg_per_sec"`
	MessageSizeBytes  int    `yaml:"message_size_bytes" json:"message_size_bytes"`
	DurationSeconds   int    `yaml:"duration_seconds"   json:"duration_seconds"`
	KeyStrategy       string `yaml:"key_strategy"       json:"key_strategy"`
	ValueStrategy     string `yaml:"value_strategy"     json:"value_strategy"`
}

type DashboardConfig struct {
	Host string `yaml:"host" json:"host"`
	Port int    `yaml:"port" json:"port"`
}

type StorageConfig struct {
	DataDir               string `yaml:"data_dir"                json:"data_dir"`
	MetricsRetentionHours int    `yaml:"metrics_retention_hours" json:"metrics_retention_hours"`
	RotationSizeMB        int    `yaml:"rotation_size_mb"        json:"rotation_size_mb"`
}

func Load(path string) (*Config, error) {
	cfg := defaults()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
	}

	applyEnvOverrides(cfg)
	return cfg, nil
}

func defaults() *Config {
	return &Config{
		Kafka: KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Version: "3.6.0",
		},
		LoadTest: LoadTestConfig{
			Topic:             "load-test",
			Partitions:        3,
			ReplicationFactor: 1,
			Workers:           10,
			TargetMsgPerSec:   1000,
			MessageSizeBytes:  1024,
			DurationSeconds:   0,
			KeyStrategy:       "random",
			ValueStrategy:     "random",
		},
		Dashboard: DashboardConfig{
			Host: "0.0.0.0",
			Port: 8080,
		},
		Storage: StorageConfig{
			DataDir:               "/data",
			MetricsRetentionHours: 24,
			RotationSizeMB:        50,
		},
	}
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		cfg.Kafka.Brokers = strings.Split(v, ",")
	}
	if v := os.Getenv("KAFKA_SASL_ENABLED"); v == "true" {
		cfg.Kafka.SASL.Enabled = true
	}
	if v := os.Getenv("KAFKA_SASL_USERNAME"); v != "" {
		cfg.Kafka.SASL.Username = v
	}
	if v := os.Getenv("KAFKA_SASL_PASSWORD"); v != "" {
		cfg.Kafka.SASL.Password = v
	}
	if v := os.Getenv("KAFKA_TLS_ENABLED"); v == "true" {
		cfg.Kafka.TLS.Enabled = true
	}
	if v := os.Getenv("LOAD_TOPIC"); v != "" {
		cfg.LoadTest.Topic = v
	}
	if v := os.Getenv("LOAD_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.LoadTest.Workers = n
		}
	}
	if v := os.Getenv("LOAD_MSG_PER_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.LoadTest.TargetMsgPerSec = n
		}
	}
	if v := os.Getenv("LOAD_MSG_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.LoadTest.MessageSizeBytes = n
		}
	}
	if v := os.Getenv("DASHBOARD_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Dashboard.Port = n
		}
	}
	if v := os.Getenv("DATA_DIR"); v != "" {
		cfg.Storage.DataDir = v
	}
}
