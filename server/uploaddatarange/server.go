package uploaddatarange

import (
	"github.com/jackc/pgx/v5/pgxpool"
)

type UploadDatarangeServer struct {
	db *pgxpool.Pool
}

func NewServer(db *pgxpool.Pool) *UploadDatarangeServer {
	return &UploadDatarangeServer{db: db}
}
