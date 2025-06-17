package server

import (
	"github.com/draganm/datas3t2/server/addbucket"
	"github.com/draganm/datas3t2/server/adddatas3t"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	*addbucket.AddBucketServer
	*adddatas3t.AddDatas3tServer
}

func NewServer(db *pgxpool.Pool) *Server {
	return &Server{
		AddBucketServer:  addbucket.NewServer(db),
		AddDatas3tServer: adddatas3t.NewServer(db),
	}
}
