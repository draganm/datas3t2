package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/draganm/datas3t2/server/adddatas3t"
)

func (a *api) addDatas3t(w http.ResponseWriter, r *http.Request) {
	var req adddatas3t.AddDatas3tRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		var validationErr adddatas3t.ValidationError
		if errors.As(err, &validationErr) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
