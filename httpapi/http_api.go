package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/draganm/datas3t2/server"
)

type api struct {
	s   *server.Server
	log *slog.Logger
}

func NewHTTPAPI(s *server.Server, log *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	a := &api{s: s, log: log}

	mux.HandleFunc("GET /api/v1/buckets", a.listBuckets)
	mux.HandleFunc("POST /api/v1/buckets", a.addBucket)
	mux.HandleFunc("GET /api/v1/datas3ts", a.listDatas3ts)
	mux.HandleFunc("POST /api/v1/datas3ts", a.addDatas3t)
	mux.HandleFunc("POST /api/v1/upload-datarange", a.startDatarangeUpload)
	mux.HandleFunc("POST /api/v1/upload-datarange/complete", a.completeDatarangeUpload)
	mux.HandleFunc("POST /api/v1/upload-datarange/cancel", a.cancelDatarangeUpload)
	return mux
}
