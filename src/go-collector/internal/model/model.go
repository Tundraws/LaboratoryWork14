package model

import "time"

type PassengerEvent struct {
	SensorID   string    `json:"sensor_id"`
	StationID  string    `json:"station_id"`
	Line       string    `json:"line"`
	Direction  string    `json:"direction"`
	EventType  string    `json:"event_type"`
	Passengers int       `json:"passengers"`
	Timestamp  time.Time `json:"timestamp"`
}

type WindowAggregate struct {
	WindowStart    time.Time `json:"window_start"`
	WindowEnd      time.Time `json:"window_end"`
	StationID      string    `json:"station_id"`
	Line           string    `json:"line"`
	Entries        int64     `json:"entries"`
	Exits          int64     `json:"exits"`
	NetFlow        int64     `json:"net_flow"`
	EventsCount    int64     `json:"events_count"`
	AveragePerTick float64   `json:"average_per_tick"`
}
