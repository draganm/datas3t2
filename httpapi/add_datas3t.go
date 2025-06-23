package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/draganm/datas3t2/server/datas3t"
)

func (a *api) addDatas3t(w http.ResponseWriter, r *http.Request) {
	var req datas3t.AddDatas3tRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		var validationErr datas3t.ValidationError
		if errors.As(err, &validationErr) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = a.s.AddDatas3t(r.Context(), a.log, &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNoContent)
}
