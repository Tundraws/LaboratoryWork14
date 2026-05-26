package simulator

import (
	"context"
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/tundraws/laboratorywork14/src/go-collector/internal/model"
	"github.com/tundraws/laboratorywork14/src/go-collector/internal/validation"
)

type Sensor struct {
	ID        string
	StationID string
	Line      string
	Direction string
}

type Simulator struct {
	sensors   []Sensor
	rate      int
	validator validation.Validator
	random    *rand.Rand
}

func New(sensors []Sensor, rate int, validator validation.Validator, seed int64) *Simulator {
	return &Simulator{
		sensors:   sensors,
		rate:      rate,
		validator: validator,
		random:    rand.New(rand.NewSource(seed)),
	}
}

func DefaultSensors() []Sensor {
	return []Sensor{
		{ID: "S-001", StationID: "central", Line: "red", Direction: "north"},
		{ID: "S-002", StationID: "central", Line: "red", Direction: "south"},
		{ID: "S-003", StationID: "park", Line: "green", Direction: "east"},
		{ID: "S-004", StationID: "park", Line: "green", Direction: "west"},
		{ID: "S-005", StationID: "river", Line: "blue", Direction: "north"},
		{ID: "S-006", StationID: "river", Line: "blue", Direction: "south"},
	}
}

func FilterByShard(sensors []Sensor, shard int, totalShards int) []Sensor {
	filtered := make([]Sensor, 0, len(sensors))
	for _, sensor := range sensors {
		if int(stableHash(sensor.ID)%uint32(totalShards)) == shard {
			filtered = append(filtered, sensor)
		}
	}
	return filtered
}

func (s *Simulator) Stream(ctx context.Context) <-chan model.PassengerEvent {
	out := make(chan model.PassengerEvent, s.rate)
	go func() {
		defer close(out)
		if len(s.sensors) == 0 {
			return
		}
		interval := time.Second / time.Duration(s.rate)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				event := s.nextEvent(now.UTC())
				if s.validator.Validate(event) == nil {
					out <- event
				}
			}
		}
	}()
	return out
}

func (s *Simulator) nextEvent(now time.Time) model.PassengerEvent {
	sensor := s.sensors[s.random.Intn(len(s.sensors))]
	eventType := "entry"
	if s.random.Intn(100) < 44 {
		eventType = "exit"
	}
	base := 1 + s.random.Intn(7)
	if now.Hour() >= 7 && now.Hour() <= 9 || now.Hour() >= 17 && now.Hour() <= 19 {
		base += 3 + s.random.Intn(12)
	}
	return model.PassengerEvent{
		SensorID:   sensor.ID,
		StationID:  sensor.StationID,
		Line:       sensor.Line,
		Direction:  sensor.Direction,
		EventType:  eventType,
		Passengers: base,
		Timestamp:  now,
	}
}

func stableHash(value string) uint32 {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(value))
	return hasher.Sum32()
}
