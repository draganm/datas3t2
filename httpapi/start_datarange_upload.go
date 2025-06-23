package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/draganm/datas3t2/server/uploaddatarange"
)

func (a *api) startDatarangeUpload(w http.ResponseWriter, r *http.Request) {

	req := &uploaddatarange.UploadDatarangeRequest{}
	err := json.NewDecoder(r.Body).Decode(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = req.Validate(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := a.s.StartDatarangeUpload(r.Context(), a.log, req)
	switch {
	case errors.Is(err, uploaddatarange.ErrDatarangeOverlap):
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}

}
