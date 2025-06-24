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

func NewServer(db *pgxpool.Pool, cacheDir string, maxCacheSize int64, encryptionKey string) (*Server, error) {
	bucketServer, err := bucket.NewServer(db, encryptionKey)
	if err != nil {
		return nil, err
	}

	datas3tServer, err := datas3t.NewServer(db, encryptionKey)
	if err != nil {
		return nil, err
	}

	datarangesServer, err := dataranges.NewServer(db, encryptionKey)
	if err != nil {
		return nil, err
	}

	downloadServer, err := download.NewServer(db, cacheDir, maxCacheSize, encryptionKey)
	if err != nil {
		return nil, err
	}

	return &Server{
		BucketServer:          bucketServer,
		Datas3tServer:         datas3tServer,
		UploadDatarangeServer: datarangesServer,
		DownloadServer:        downloadServer,
	}, nil
}
