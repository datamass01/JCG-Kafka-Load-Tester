package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	KafkaInstances []KafkaInstanceConfig `yaml:"kafka_instances" json:"kafka_instances"`
	ActiveInstance string                `yaml:"active_instance" json:"active_instance"`
	Kafka          KafkaConfig           `yaml:"kafka"           json:"kafka"` // legacy single-instance
	LoadTest       LoadTestConfig        `yaml:"load_test"       json:"load_test"`
	ConsumerTest   ConsumerTestConfig    `yaml:"consumer_test"   json:"consumer_test"`
	Dashboard      DashboardConfig       `yaml:"dashboard"       json:"dashboard"`
	Storage        StorageConfig         `yaml:"storage"         json:"storage"`
	Security       SecurityConfig        `yaml:"security"        json:"-"`
}

// SecurityConfig controls auth, CORS and WebSocket origin enforcement.
// Always serialised as the JSON zero value so secrets never leak via /api/config.
type SecurityConfig struct {
	AuthToken           string   `yaml:"auth_token"            json:"-"`
	AllowedOrigins      []string `yaml:"allowed_origins"       json:"-"`
	AllowedBrokerHosts  []string `yaml:"allowed_broker_hosts"  json:"-"`
	AllowPrivateBrokers bool     `yaml:"allow_private_brokers" json:"-"`
}

// Redacted returns a deep copy of the config with all secret-bearing fields
// (SASL credentials, TLS material) scrubbed. Use this for any response served
// to a network caller.
func (c *Config) Redacted() *Config {
	cp := *c
	cp.Kafka = redactKafka(cp.Kafka)
	cp.KafkaInstances = make([]KafkaInstanceConfig, len(c.KafkaInstances))
	for i, inst := range c.KafkaInstances {
		ri := inst
		ri.SASL = redactSASL(inst.SASL)
		ri.TLS = redactTLS(inst.TLS)
		cp.KafkaInstances[i] = ri
	}
	cp.Security = SecurityConfig{}
	return &cp
}

func redactKafka(k KafkaConfig) KafkaConfig {
	k.SASL = redactSASL(k.SASL)
	k.TLS = redactTLS(k.TLS)
	return k
}

func redactSASL(s SASLConfig) SASLConfig {
	if s.Username != "" {
		s.Username = "***"
	}
	if s.Password != "" {
		s.Password = "***"
	}
	return s
}

func redactTLS(t TLSConfig) TLSConfig {
	if t.CACert != "" {
		t.CACert = "***"
	}
	if t.ClientCert != "" {
		t.ClientCert = "***"
	}
	if t.ClientKey != "" {
		t.ClientKey = "***"
	}
	return t
}

// KafkaInstanceConfig is a named Kafka cluster endpoint.
type KafkaInstanceConfig struct {
	Name    string     `yaml:"name"    json:"name"`
	Brokers []string   `yaml:"brokers" json:"brokers"`
	SASL    SASLConfig `yaml:"sasl"    json:"sasl"`
	TLS     TLSConfig  `yaml:"tls"     json:"tls"`
	Version string     `yaml:"version" json:"version"`
	AdHoc   bool       `yaml:"-"       json:"-"` // runtime-only; not persisted to YAML
}

// ToKafkaConfig converts to the legacy KafkaConfig for use with kafka package functions.
func (k *KafkaInstanceConfig) ToKafkaConfig() *KafkaConfig {
	return &KafkaConfig{
		Brokers: k.Brokers,
		SASL:    k.SASL,
		TLS:     k.TLS,
		Version: k.Version,
	}
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
	Topic             string `yaml:"topic"               json:"topic"`
	Partitions        int    `yaml:"partitions"          json:"partitions"`
	ReplicationFactor int    `yaml:"replication_factor"  json:"replication_factor"`
	MinInsyncReplicas int    `yaml:"min_insync_replicas" json:"min_insync_replicas"`
	Workers           int    `yaml:"workers"             json:"workers"`
	TargetMsgPerSec   int    `yaml:"target_msg_per_sec"  json:"target_msg_per_sec"`
	MessageSizeBytes  int    `yaml:"message_size_bytes"  json:"message_size_bytes"`
	DurationSeconds   int    `yaml:"duration_seconds"    json:"duration_seconds"`
	KeyStrategy       string `yaml:"key_strategy"        json:"key_strategy"`
	ValueStrategy     string `yaml:"value_strategy"      json:"value_strategy"`
}

type ConsumerTestConfig struct {
	Topic           string `yaml:"topic"            json:"topic"`
	ConsumerGroup   string `yaml:"consumer_group"   json:"consumer_group"`
	DurationSeconds int    `yaml:"duration_seconds" json:"duration_seconds"`
	OffsetReset     string `yaml:"offset_reset"     json:"offset_reset"`
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
	normalize(cfg)
	return cfg, nil
}

// normalize ensures KafkaInstances is always populated and ActiveInstance is valid.
// If no instances are defined, one is synthesised from the legacy kafka section.
func normalize(cfg *Config) {
	if len(cfg.KafkaInstances) == 0 {
		cfg.KafkaInstances = []KafkaInstanceConfig{{
			Name:    "default",
			Brokers: cfg.Kafka.Brokers,
			SASL:    cfg.Kafka.SASL,
			TLS:     cfg.Kafka.TLS,
			Version: cfg.Kafka.Version,
		}}
	}
	if cfg.ActiveInstance == "" || cfg.FindInstance(cfg.ActiveInstance) == nil {
		cfg.ActiveInstance = cfg.KafkaInstances[0].Name
	}
}

// ActiveKafkaInstance returns the currently active instance config.
func (c *Config) ActiveKafkaInstance() *KafkaInstanceConfig {
	return c.FindInstance(c.ActiveInstance)
}

// FindInstance returns the named instance or nil if not found.
func (c *Config) FindInstance(name string) *KafkaInstanceConfig {
	for i := range c.KafkaInstances {
		if c.KafkaInstances[i].Name == name {
			return &c.KafkaInstances[i]
		}
	}
	return nil
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
			ReplicationFactor: 3,
			MinInsyncReplicas: 2,
			Workers:           10,
			TargetMsgPerSec:   1000,
			MessageSizeBytes:  1024,
			DurationSeconds:   0,
			KeyStrategy:       "random",
			ValueStrategy:     "random",
		},
		ConsumerTest: ConsumerTestConfig{
			Topic:           "load-test",
			ConsumerGroup:   "kafka-load-test",
			DurationSeconds: 0,
			OffsetReset:     "latest",
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
	if v := os.Getenv("DASHBOARD_AUTH_TOKEN"); v != "" {
		cfg.Security.AuthToken = v
	}
	if v := os.Getenv("DASHBOARD_ALLOWED_ORIGINS"); v != "" {
		cfg.Security.AllowedOrigins = strings.Split(v, ",")
		for i := range cfg.Security.AllowedOrigins {
			cfg.Security.AllowedOrigins[i] = strings.TrimSpace(cfg.Security.AllowedOrigins[i])
		}
	}
	if v := os.Getenv("DASHBOARD_ALLOWED_BROKER_HOSTS"); v != "" {
		cfg.Security.AllowedBrokerHosts = strings.Split(v, ",")
		for i := range cfg.Security.AllowedBrokerHosts {
			cfg.Security.AllowedBrokerHosts[i] = strings.TrimSpace(cfg.Security.AllowedBrokerHosts[i])
		}
	}
	if v := os.Getenv("DASHBOARD_ALLOW_PRIVATE_BROKERS"); v == "true" {
		cfg.Security.AllowPrivateBrokers = true
	}
}
