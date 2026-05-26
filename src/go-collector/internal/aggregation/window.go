package aggregation

import (
	"context"
	"time"

	"github.com/tundraws/laboratorywork14/src/go-collector/internal/model"
)

type WindowAggregator struct {
	duration time.Duration
}

type bucketKey struct {
	stationID string
	line      string
}

type bucket struct {
	entries int64
	exits   int64
	events  int64
}

func NewWindowAggregator(duration time.Duration) *WindowAggregator {
	return &WindowAggregator{duration: duration}
}

func (a *WindowAggregator) Aggregate(ctx context.Context, events <-chan model.PassengerEvent) <-chan []model.WindowAggregate {
	out := make(chan []model.WindowAggregate, 4)
	go func() {
		defer close(out)
		ticker := time.NewTicker(a.duration)
		defer ticker.Stop()
		windowStart := time.Now().UTC().Truncate(a.duration)
		buckets := make(map[bucketKey]bucket)

		flush := func(now time.Time) {
			if len(buckets) == 0 {
				windowStart = now.UTC().Truncate(a.duration)
				return
			}
			aggregates := make([]model.WindowAggregate, 0, len(buckets))
			windowEnd := windowStart.Add(a.duration)
			for key, value := range buckets {
				total := value.entries + value.exits
				average := 0.0
				if value.events > 0 {
					average = float64(total) / float64(value.events)
				}
				aggregates = append(aggregates, model.WindowAggregate{
					WindowStart:    windowStart,
					WindowEnd:      windowEnd,
					StationID:      key.stationID,
					Line:           key.line,
					Entries:        value.entries,
					Exits:          value.exits,
					NetFlow:        value.entries - value.exits,
					EventsCount:    value.events,
					AveragePerTick: average,
				})
			}
			out <- aggregates
			buckets = make(map[bucketKey]bucket)
			windowStart = now.UTC().Truncate(a.duration)
		}

		for {
			select {
			case <-ctx.Done():
				flush(time.Now().UTC())
				return
			case now := <-ticker.C:
				flush(now)
			case event, ok := <-events:
				if !ok {
					flush(time.Now().UTC())
					return
				}
				key := bucketKey{stationID: event.StationID, line: event.Line}
				current := buckets[key]
				if event.EventType == "entry" {
					current.entries += int64(event.Passengers)
				} else {
					current.exits += int64(event.Passengers)
				}
				current.events++
				buckets[key] = current
			}
		}
	}()
	return out
}
