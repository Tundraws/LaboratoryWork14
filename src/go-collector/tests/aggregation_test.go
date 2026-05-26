package tests

import (
	"context"
	"testing"
	"time"

	"github.com/tundraws/laboratorywork14/src/go-collector/internal/aggregation"
	"github.com/tundraws/laboratorywork14/src/go-collector/internal/model"
)

func TestWindowAggregatorComputesFlowProperties(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan model.PassengerEvent, 4)
	aggregator := aggregation.NewWindowAggregator(50 * time.Millisecond)
	out := aggregator.Aggregate(ctx, events)

	now := time.Now().UTC()
	events <- model.PassengerEvent{StationID: "central", Line: "red", EventType: "entry", Passengers: 10, Timestamp: now}
	events <- model.PassengerEvent{StationID: "central", Line: "red", EventType: "exit", Passengers: 3, Timestamp: now}
	close(events)
	defer cancel()

	batch := <-out
	if len(batch) != 1 {
		t.Fatalf("expected one aggregate, got %d", len(batch))
	}
	got := batch[0]
	if got.Entries <= got.Exits {
		t.Fatalf("expected entries to be greater than exits: %+v", got)
	}
	if got.NetFlow != got.Entries-got.Exits {
		t.Fatalf("net flow must equal entries minus exits: %+v", got)
	}
	if got.AveragePerTick <= 0 {
		t.Fatalf("average per tick must be positive: %+v", got)
	}
}
