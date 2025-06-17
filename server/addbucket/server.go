package addbucket

import (
	"github.com/jackc/pgx/v5/pgxpool"
)

type AddBucketServer struct {
	db *pgxpool.Pool
}

func NewServer(db *pgxpool.Pool) *AddBucketServer {
	return &AddBucketServer{db: db}
}
