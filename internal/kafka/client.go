package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/IBM/sarama"
	"kafka-agent/internal/config"
)

func NewSaramaConfig(cfg *config.KafkaConfig) (*sarama.Config, error) {
	sc := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(cfg.Version)
	if err != nil {
		version = sarama.V3_6_0_0
	}
	sc.Version = version

	sc.Producer.Return.Successes = true
	sc.Producer.Return.Errors = true
	sc.Producer.RequiredAcks = sarama.WaitForAll
	sc.Producer.Compression = sarama.CompressionSnappy

	// Retry window must outlast a worst-case KRaft leader election. Under load
	// these have been observed to take 60–90 s, so we allow ~90 s before a
	// message is surfaced as failed: 90 retries × 1 s.
	sc.Producer.Retry.Max = 90
	sc.Producer.Retry.Backoff = 1 * time.Second

	sc.Metadata.Retry.Max = 10
	sc.Metadata.Retry.Backoff = 1 * time.Second
	sc.Metadata.RefreshFrequency = 5 * time.Second

	// Bound network operations so a dead broker cannot wedge a goroutine.
	sc.Net.DialTimeout = 10 * time.Second
	sc.Net.ReadTimeout = 10 * time.Second
	sc.Net.WriteTimeout = 10 * time.Second

	if cfg.TLS.Enabled {
		tlsCfg, err := buildTLSConfig(&cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("tls config: %w", err)
		}
		sc.Net.TLS.Enable = true
		sc.Net.TLS.Config = tlsCfg
	}

	if cfg.SASL.Enabled {
		sc.Net.SASL.Enable = true
		sc.Net.SASL.User = cfg.SASL.Username
		sc.Net.SASL.Password = cfg.SASL.Password
		switch cfg.SASL.Mechanism {
		case "SCRAM-SHA-256":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			sc.Net.SASL.SCRAMClientGeneratorFunc = sha256Generator
		case "SCRAM-SHA-512":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			sc.Net.SASL.SCRAMClientGeneratorFunc = sha512Generator
		default:
			sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
	}

	return sc, nil
}

func NewClient(brokers []string, sc *sarama.Config) (sarama.Client, error) {
	return sarama.NewClient(brokers, sc)
}

func buildTLSConfig(cfg *config.TLSConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.SkipVerify, //nolint:gosec — user-configured
	}

	if cfg.CACert != "" {
		caCert, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("read ca cert: %w", err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caCert)
		tlsCfg.RootCAs = pool
	}

	if cfg.ClientCert != "" && cfg.ClientKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}
