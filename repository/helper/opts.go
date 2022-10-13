package helper

import (
	"encoding/json"

	"github.com/akrennmair/slice"
	"github.com/hnhuaxi/domain/repository"
	pb "github.com/hnhuaxi/platform/proto"
	"go.uber.org/multierr"
)

type (
	Paginater interface {
		GetPage() *pb.Paginate
	}

	MultiFilter interface {
		GetFilters() []*pb.Filter
	}

	GetFilter interface {
		GetFilters() []string
	}

	MultiSorter interface {
		GetSorts() []*pb.Sort
	}

	GetSort interface {
		GetSorts() []string
	}
)

func BuildSearchOpts(req interface{}) (*repository.SearchOpt, error) {
	var (
		searchOpt repository.SearchOpt
		opts      []repository.SearchOptFunc
	)

	chain := func(opt repository.SearchOptFunc) {
		opts = append(opts, opt)
	}

	if pager, ok := req.(Paginater); ok {
		if page := pager.GetPage(); page != nil {
			if page.AfterId != nil {
				chain(repository.OptAfter(repository.SID(*page.AfterId)))
			}

			if page.BeforeId != nil {
				chain(repository.OptBefore(repository.SID(*page.BeforeId)))
			}

			// if page.Offset != nil {
			// 	chain(repository.OptOffset(int(*page.Offset)))
			// }

			if page.Page != nil {
				chain(repository.OptPage(int(*page.Page)))
			}

			if page.PageSize != nil {
				chain(repository.OptPageSize(int(*page.PageSize)))
			}
		}
	}

	if filter, ok := req.(MultiFilter); ok {

		if filters := filter.GetFilters(); len(filters) > 0 {

			filterOpts := slice.Map(filters, func(flt *pb.Filter) repository.FilterItem {

				return repository.FilterItem{
					ID: flt.Id,
					// Value: flt.Value.,
				}
			})

			chain(repository.OptFilter(filterOpts...))
		}
	}

	if filter, ok := req.(GetFilter); ok {
		var errs error
		if filters := filter.GetFilters(); len(filters) > 0 {
			filters = slice.Filter(filters, func(f string) bool {
				return len(f) > 0
			})
			filterOpts := slice.Map(filters, func(flt string) repository.FilterItem {
				var filterItem repository.FilterItem
				if err := json.Unmarshal([]byte(flt), &filterItem); err != nil {
					errs = multierr.Append(errs, err)
				}

				return filterItem
			})

			chain(repository.OptFilter(filterOpts...))
		}

		if errs != nil {
			return nil, errs
		}
	}

	if sorter, ok := req.(MultiSorter); ok {
		if sorts := sorter.GetSorts(); len(sorts) > 0 {
			sortsOpts := slice.Map(sorts, func(flt *pb.Sort) repository.SortMode {
				return repository.SortMode{
					Field: flt.Field,
					// Direction: repository.OrderDirection(),
				}
			})

			chain(repository.OptSort(sortsOpts...))
		}
	}

	if sorter, ok := req.(GetSort); ok {
		var errs error
		if sorts := sorter.GetSorts(); len(sorts) > 0 {

			sortsOpts := slice.Map(sorts, func(sort string) repository.SortMode {
				var item repository.SortMode
				if err := json.Unmarshal([]byte(sort), &item); err != nil {
					errs = multierr.Append(errs, err)
				}

				return item
			})

			chain(repository.OptSort(sortsOpts...))
		}

		if errs != nil {
			return nil, errs
		}
	}

	for _, op := range opts {
		if err := op(&searchOpt); err != nil {
			return nil, err
		}
	}

	return &searchOpt, nil
}
