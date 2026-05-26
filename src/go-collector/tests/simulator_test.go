package tests

import (
	"testing"

	"github.com/tundraws/laboratorywork14/src/go-collector/internal/simulator"
)

func TestFilterByShardAssignsEverySensorOnce(t *testing.T) {
	sensors := simulator.DefaultSensors()
	seen := make(map[string]bool, len(sensors))

	for shard := 0; shard < 3; shard++ {
		for _, sensor := range simulator.FilterByShard(sensors, shard, 3) {
			if seen[sensor.ID] {
				t.Fatalf("sensor %s assigned to more than one shard", sensor.ID)
			}
			seen[sensor.ID] = true
		}
	}

	if len(seen) != len(sensors) {
		t.Fatalf("expected all sensors to be assigned, got %d of %d", len(seen), len(sensors))
	}
}
