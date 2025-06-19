package httpapi

import (
	"net/http"

	"github.com/draganm/datas3t2/server"
)

type api struct {
	s *server.Server
}

func NewHTTPAPI(s *server.Server) *http.ServeMux {
	mux := http.NewServeMux()
	a := &api{s: s}

	mux.HandleFunc("POST /api/v1/buckets", a.addBucket)
	mux.HandleFunc("POST /api/v1/datas3ts", a.addDatas3t)
	mux.HandleFunc("POST /api/v1/upload-datarange", a.startDatarangeUpload)
	mux.HandleFunc("POST /api/v1/upload-datarange/complete", a.completeDatarangeUpload)
	return mux
}
