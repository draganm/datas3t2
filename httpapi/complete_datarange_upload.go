package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/draganm/datas3t2/server/uploaddatarange"
)

func (a *api) completeDatarangeUpload(w http.ResponseWriter, r *http.Request) {

	req := &uploaddatarange.CompleteUploadRequest{}
	err := json.NewDecoder(r.Body).Decode(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = a.s.CompleteDatarangeUpload(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
}
