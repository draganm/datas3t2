package httpapi

import (
	"encoding/json"
	"net/http"
)

func (a *api) listBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := a.s.ListBuckets(r.Context(), a.log)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(buckets)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
