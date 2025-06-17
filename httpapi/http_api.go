package httpapi

import (
	"encoding/json"
	"errors"
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
	return mux
}

func (a *api) addBucket(w http.ResponseWriter, r *http.Request) {
	var req server.BucketInfo
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = a.s.AddBucket(r.Context(), &req)
	if err != nil {
		var validationErr server.ValidationError
		if errors.As(err, &validationErr) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}
