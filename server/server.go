package server

import (
	"github.com/draganm/datas3t2/server/bucket"
	"github.com/draganm/datas3t2/server/dataranges"
	"github.com/draganm/datas3t2/server/datas3t"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	*bucket.BucketServer
	*datas3t.Datas3tServer
	*dataranges.UploadDatarangeServer
}

func NewServer(db *pgxpool.Pool) *Server {
	return &Server{
		BucketServer:          bucket.NewServer(db),
		Datas3tServer:         datas3t.NewServer(db),
		UploadDatarangeServer: dataranges.NewServer(db),
	}
}
