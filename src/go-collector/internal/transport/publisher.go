package transport

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/tundraws/laboratorywork14/src/go-collector/internal/model"
)

type Publisher interface {
	Publish(batch []model.WindowAggregate)
	Close()
}

type NATSPublisher struct {
	conn    *nats.Conn
	subject string
	logger  *slog.Logger
}

type NoopPublisher struct {
	logger *slog.Logger
}

func NewNATSPublisher(url string, subject string, logger *slog.Logger) (Publisher, error) {
	conn, err := nats.Connect(url, nats.Timeout(2*time.Second), nats.RetryOnFailedConnect(false))
	if err != nil {
		return NoopPublisher{logger: logger}, err
	}
	return &NATSPublisher{conn: conn, subject: subject, logger: logger}, nil
}

func (p *NATSPublisher) Publish(batch []model.WindowAggregate) {
	payload, err := json.Marshal(batch)
	if err != nil {
		p.logger.Warn("failed to marshal aggregate batch", slog.String("error", err.Error()))
		return
	}
	if err := p.conn.Publish(p.subject, payload); err != nil {
		p.logger.Warn("failed to publish aggregate batch", slog.String("error", err.Error()))
	}
}

func (p *NATSPublisher) Close() {
	p.conn.Close()
}

func (p NoopPublisher) Publish(batch []model.WindowAggregate) {
	p.logger.Info("nats unavailable, keeping aggregates only in memory", slog.Int("records", len(batch)))
}

func (NoopPublisher) Close() {}
