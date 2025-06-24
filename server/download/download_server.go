package download

import (
	"os"
	"path/filepath"

	"github.com/draganm/datas3t2/tarindex/diskcache"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DownloadServer struct {
	pgxPool   *pgxpool.Pool
	diskCache *diskcache.IndexDiskCache
}

func NewDownloadServer(pgxPool *pgxpool.Pool) (*DownloadServer, error) {
	// Create a temporary directory for the disk cache
	// In production, this should be configurable
	cacheDir := filepath.Join(os.TempDir(), "datas3t2-cache")

	// 1GB cache size (configurable in production)
	maxCacheSize := int64(1024 * 1024 * 1024)

	diskCache, err := diskcache.NewIndexDiskCache(cacheDir, maxCacheSize)
	if err != nil {
		return nil, err
	}

	return &DownloadServer{
		pgxPool:   pgxPool,
		diskCache: diskCache,
	}, nil
}

func (s *DownloadServer) Close() error {
	if s.diskCache != nil {
		return s.diskCache.Close()
	}
	return nil
}
