package validation

import (
	"errors"
	"strings"

	"github.com/tundraws/laboratorywork14/src/go-collector/internal/model"
)

type Validator interface {
	Validate(event model.PassengerEvent) error
}

type PassengerEventValidator struct{}

func (PassengerEventValidator) Validate(event model.PassengerEvent) error {
	if strings.TrimSpace(event.SensorID) == "" {
		return errors.New("sensor id is required")
	}
	if strings.TrimSpace(event.StationID) == "" {
		return errors.New("station id is required")
	}
	if event.EventType != "entry" && event.EventType != "exit" {
		return errors.New("event type must be entry or exit")
	}
	if event.Passengers < 0 || event.Passengers > 500 {
		return errors.New("passenger count is outside supported sensor range")
	}
	if event.Timestamp.IsZero() {
		return errors.New("timestamp is required")
	}
	return nil
}
