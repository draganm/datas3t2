package adddatas3t

import (
	"github.com/jackc/pgx/v5/pgxpool"
)

type AddDatas3tServer struct {
	db *pgxpool.Pool
}

func NewServer(db *pgxpool.Pool) *AddDatas3tServer {
	return &AddDatas3tServer{db: db}
}
