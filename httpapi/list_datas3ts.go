package httpapi

import (
	"encoding/json"
	"net/http"
)

func (a *api) listDatas3ts(w http.ResponseWriter, r *http.Request) {
	datas3ts, err := a.s.ListDatas3ts(r.Context(), a.log)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(datas3ts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
