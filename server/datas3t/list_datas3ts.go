package datas3t

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/draganm/datas3t2/postgresstore"
	"github.com/jackc/pgx/v5/pgtype"
)

type Datas3tInfo struct {
	DatasetName      string `json:"dataset_name"`
	BucketName       string `json:"bucket_name"`
	DatarangeCount   int64  `json:"datarange_count"`
	TotalDatapoints  int64  `json:"total_datapoints"`
	LowestDatapoint  int64  `json:"lowest_datapoint"`
	HighestDatapoint int64  `json:"highest_datapoint"`
	TotalBytes       int64  `json:"total_bytes"`
}

// Helper function to convert interface{} to int64
func toInt64(v interface{}) int64 {
	if v == nil {
		return 0
	}

	switch val := v.(type) {
	case int64:
		return val
	case int32:
		return int64(val)
	case int:
		return int64(val)
	case float64:
		return int64(val)
	case float32:
		return int64(val)
	case pgtype.Numeric:
		// Handle PostgreSQL numeric type
		if !val.Valid {
			return 0
		}
		// Convert pgtype.Numeric to int64 using Int64Value method
		intVal, err := val.Int64Value()
		if err != nil {
			return 0
		}
		return intVal.Int64
	default:
		return 0
	}
}

func (s *Datas3tServer) ListDatas3ts(ctx context.Context, log *slog.Logger) ([]Datas3tInfo, error) {
	log.Info("Listing datas3ts")

	queries := postgresstore.New(s.db)

	rows, err := queries.ListDatas3ts(ctx)
	if err != nil {
		log.Error("Failed to list datas3ts", "error", err)
		return nil, fmt.Errorf("failed to list datas3ts: %w", err)
	}

	datas3ts := make([]Datas3tInfo, 0, len(rows))

	for _, row := range rows {
		datas3tInfo := Datas3tInfo{
			DatasetName:      row.DatasetName,
			BucketName:       row.BucketName,
			DatarangeCount:   toInt64(row.DatarangeCount),
			TotalDatapoints:  toInt64(row.TotalDatapoints),
			LowestDatapoint:  toInt64(row.LowestDatapoint),
			HighestDatapoint: toInt64(row.HighestDatapoint),
			TotalBytes:       toInt64(row.TotalBytes),
		}

		datas3ts = append(datas3ts, datas3tInfo)
	}

	log.Info("Listed datas3ts", "count", len(datas3ts))
	return datas3ts, nil
}
