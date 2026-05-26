package tests

import (
	"testing"
	"time"

	"github.com/tundraws/laboratorywork14/src/go-collector/internal/model"
	"github.com/tundraws/laboratorywork14/src/go-collector/internal/validation"
)

func TestPassengerEventValidatorRejectsInvalidEvents(t *testing.T) {
	valid := model.PassengerEvent{
		SensorID:   "S-001",
		StationID:  "central",
		Line:       "red",
		Direction:  "north",
		EventType:  "entry",
		Passengers: 12,
		Timestamp:  time.Now().UTC(),
	}
	tests := map[string]model.PassengerEvent{
		"missing sensor":        withSensor(valid, ""),
		"invalid event type":    withEventType(valid, "transfer"),
		"negative passengers":   withPassengers(valid, -1),
		"sensor range exceeded": withPassengers(valid, 501),
	}

	validator := validation.PassengerEventValidator{}
	for name, event := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validator.Validate(event); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestPassengerEventValidatorAcceptsValidEvent(t *testing.T) {
	event := model.PassengerEvent{
		SensorID:   "S-001",
		StationID:  "central",
		Line:       "red",
		Direction:  "north",
		EventType:  "exit",
		Passengers: 3,
		Timestamp:  time.Now().UTC(),
	}
	if err := (validation.PassengerEventValidator{}).Validate(event); err != nil {
		t.Fatalf("expected valid event, got %v", err)
	}
}

func withSensor(event model.PassengerEvent, sensorID string) model.PassengerEvent {
	event.SensorID = sensorID
	return event
}

func withEventType(event model.PassengerEvent, eventType string) model.PassengerEvent {
	event.EventType = eventType
	return event
}

func withPassengers(event model.PassengerEvent, passengers int) model.PassengerEvent {
	event.Passengers = passengers
	return event
}
