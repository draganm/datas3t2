package server

import (
	"github.com/draganm/datas3t2/server/bucket"
	"github.com/draganm/datas3t2/server/dataranges"
	"github.com/draganm/datas3t2/server/datas3t"
	"github.com/draganm/datas3t2/server/download"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	*bucket.BucketServer
	*datas3t.Datas3tServer
	*dataranges.UploadDatarangeServer
	*download.DownloadServer
}

func NewServer(db *pgxpool.Pool, cacheDir string, maxCacheSize int64) (*Server, error) {
	downloadServer, err := download.NewDownloadServer(db, cacheDir, maxCacheSize)
	if err != nil {
		return nil, err
	}

	return &Server{
		BucketServer:          bucket.NewServer(db),
		Datas3tServer:         datas3t.NewServer(db),
		UploadDatarangeServer: dataranges.NewServer(db),
		DownloadServer:        downloadServer,
	}, nil
}
