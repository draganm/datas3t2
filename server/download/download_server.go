package download

import (
	"github.com/draganm/datas3t2/crypto"
	"github.com/draganm/datas3t2/tarindex/diskcache"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DownloadServer struct {
	pgxPool   *pgxpool.Pool
	diskCache *diskcache.IndexDiskCache
	encryptor *crypto.CredentialEncryptor
}

func NewServer(pgxPool *pgxpool.Pool, cacheDir string, maxCacheSize int64, encryptionKey string) (*DownloadServer, error) {
	encryptor, err := crypto.NewCredentialEncryptor(encryptionKey)
	if err != nil {
		return nil, err
	}

	diskCache, err := diskcache.NewIndexDiskCache(cacheDir, maxCacheSize)
	if err != nil {
		return nil, err
	}

	return &DownloadServer{
		pgxPool:   pgxPool,
		diskCache: diskCache,
		encryptor: encryptor,
	}, nil
}

func (s *DownloadServer) Close() error {
	if s.diskCache != nil {
		return s.diskCache.Close()
	}
	return nil
}
