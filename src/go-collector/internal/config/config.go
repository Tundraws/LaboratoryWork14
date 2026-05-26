package config

import (
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	InstanceID      string
	TotalShards     int
	WindowDuration  time.Duration
	EventsPerSecond int
	ArrowAddr       string
	EtcdEndpoints   []string
	NATSURL         string
	NATSSubject     string
}

func Load() (Config, error) {
	cfg := Config{
		InstanceID:      readString("COLLECTOR_INSTANCE_ID", "collector-1"),
		TotalShards:     readInt("COLLECTOR_TOTAL_SHARDS", 3),
		WindowDuration:  time.Duration(readInt("COLLECTOR_WINDOW_SECONDS", 10)) * time.Second,
		EventsPerSecond: readInt("COLLECTOR_EVENTS_PER_SECOND", 50),
		ArrowAddr:       readString("COLLECTOR_ARROW_ADDR", ":8080"),
		EtcdEndpoints:   splitCSV(readString("COLLECTOR_ETCD_ENDPOINTS", "http://localhost:2379")),
		NATSURL:         readString("COLLECTOR_NATS_URL", "nats://localhost:4222"),
		NATSSubject:     readString("COLLECTOR_NATS_SUBJECT", "metro.passenger.windows"),
	}
	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.InstanceID) == "" {
		return errors.New("collector instance id is required")
	}
	if c.TotalShards < 1 {
		return errors.New("total shards must be positive")
	}
	if c.WindowDuration < time.Second {
		return errors.New("window duration must be at least one second")
	}
	if c.EventsPerSecond < 1 {
		return errors.New("events per second must be positive")
	}
	if c.ArrowAddr == "" {
		return errors.New("arrow address is required")
	}
	if _, err := url.Parse(c.NATSURL); err != nil {
		return err
	}
	return nil
}

func readString(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func readInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	return values
}
