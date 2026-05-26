package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/tundraws/laboratorywork14/src/go-collector/internal/model"
)

type ArrowServer struct {
	server *http.Server
	store  *AggregateStore
	logger *slog.Logger
}

func NewArrowServer(addr string, store *AggregateStore, logger *slog.Logger) *ArrowServer {
	mux := http.NewServeMux()
	server := &ArrowServer{store: store, logger: logger}
	mux.HandleFunc("/healthz", server.health)
	mux.HandleFunc("/metrics", server.metrics)
	mux.HandleFunc("/arrow", server.arrow)
	server.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return server
}

func (s *ArrowServer) ListenAndServe() error {
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *ArrowServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *ArrowServer) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *ArrowServer) metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	records := len(s.store.Snapshot())
	_, _ = fmt.Fprintf(w, "metro_aggregate_records %d\n", records)
}

func (s *ArrowServer) arrow(w http.ResponseWriter, _ *http.Request) {
	records := s.store.Snapshot()
	w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
	if err := writeArrowStream(w, records); err != nil {
		s.logger.Warn("failed to write arrow stream", slog.String("error", err.Error()))
	}
}

func writeArrowStream(w http.ResponseWriter, records []model.WindowAggregate) error {
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "window_start", Type: arrow.BinaryTypes.String},
		{Name: "window_end", Type: arrow.BinaryTypes.String},
		{Name: "station_id", Type: arrow.BinaryTypes.String},
		{Name: "line", Type: arrow.BinaryTypes.String},
		{Name: "entries", Type: arrow.PrimitiveTypes.Int64},
		{Name: "exits", Type: arrow.PrimitiveTypes.Int64},
		{Name: "net_flow", Type: arrow.PrimitiveTypes.Int64},
		{Name: "events_count", Type: arrow.PrimitiveTypes.Int64},
		{Name: "average_per_tick", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()
	for _, record := range records {
		builder.Field(0).(*array.StringBuilder).Append(record.WindowStart.Format(time.RFC3339))
		builder.Field(1).(*array.StringBuilder).Append(record.WindowEnd.Format(time.RFC3339))
		builder.Field(2).(*array.StringBuilder).Append(record.StationID)
		builder.Field(3).(*array.StringBuilder).Append(record.Line)
		builder.Field(4).(*array.Int64Builder).Append(record.Entries)
		builder.Field(5).(*array.Int64Builder).Append(record.Exits)
		builder.Field(6).(*array.Int64Builder).Append(record.NetFlow)
		builder.Field(7).(*array.Int64Builder).Append(record.EventsCount)
		builder.Field(8).(*array.Float64Builder).Append(record.AveragePerTick)
	}
	arrowRecord := builder.NewRecord()
	defer arrowRecord.Release()

	writer := ipc.NewWriter(w, ipc.WithSchema(schema))
	defer writer.Close()
	return writer.Write(arrowRecord)
}
