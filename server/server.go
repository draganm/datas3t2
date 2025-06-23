package server

import (
	"github.com/draganm/datas3t2/server/bucket"
	"github.com/draganm/datas3t2/server/datas3t"
	"github.com/draganm/datas3t2/server/uploaddatarange"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	*bucket.BucketServer
	*datas3t.AddDatas3tServer
	*uploaddatarange.UploadDatarangeServer
}

func NewServer(db *pgxpool.Pool) *Server {
	return &Server{
		BucketServer:          bucket.NewServer(db),
		AddDatas3tServer:      datas3t.NewServer(db),
		UploadDatarangeServer: uploaddatarange.NewServer(db),
	}
}
