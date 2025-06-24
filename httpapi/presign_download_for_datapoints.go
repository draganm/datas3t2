package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/draganm/datas3t2/server/download"
)

func (a *api) presignDownloadForDatapoints(w http.ResponseWriter, r *http.Request) {
	req := &download.PreSignDownloadForDatapointsRequest{}
	err := json.NewDecoder(r.Body).Decode(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := a.s.PreSignDownloadForDatapoints(r.Context(), *req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
