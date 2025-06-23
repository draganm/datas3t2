package datas3t

import (
	"github.com/jackc/pgx/v5/pgxpool"
)

type Datas3tServer struct {
	db *pgxpool.Pool
}

func NewServer(db *pgxpool.Pool) *Datas3tServer {
	return &Datas3tServer{db: db}
}
