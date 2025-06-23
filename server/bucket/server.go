package bucket

import (
	"github.com/jackc/pgx/v5/pgxpool"
)

type BucketServer struct {
	db *pgxpool.Pool
}

func NewServer(db *pgxpool.Pool) *BucketServer {
	return &BucketServer{db: db}
}
