package helper

import (
	"encoding/json"
	"net/http"

	"github.com/hnhuaxi/domain/repository"
)

func ParseFilters(r *http.Request) ([]repository.FilterItem, error) {
	panic("nonimplement")
}

func BuildFilters(filter []string) ([]repository.FilterItem, error) {
	var filters = make([]repository.FilterItem, 0)
	for _, e := range filter {
		var filter repository.FilterItem
		err := json.Unmarshal([]byte(e), &filter)
		if err != nil {
			return nil, err
		}
		filters = append(filters, filter)
	}

	return filters, nil
}

func BuildSort(sorts []string) ([]repository.SortMode, error) {
	var sortModes = make([]repository.SortMode, 0)
	for _, e := range sorts {
		var sort repository.SortMode
		err := json.Unmarshal([]byte(e), &sort)
		if err != nil {
			return nil, err
		}
		sortModes = append(sortModes, sort)
	}

	return sortModes, nil
}
