package repository

type ExtendField struct {
	table       string
	joins       ScopeFunc
	association ScopeFunc
}

func (extend *ExtendField) Table(table string) *ExtendField {
	extend.table = table
	return extend
}

func (extend *ExtendField) Joins(query string, conds ...interface{}) *ExtendField {
	extend.joins = func(tx Scope) Scope {
		return tx.Joins(query, conds...)
	}
	return extend
}

func (extend *ExtendField) Assocation(column, query string, conds ...interface{}) *ExtendField {
	extend.association = func(tx Scope) Scope {
		assoc := tx.Association(column)
		switch query {
		case "Find":
			_ = assoc
			// assoc.Find()
		}
		return tx
	}

	return extend
}

func (extend *ExtendField) Apply(fields []string, scope Scope) Scope {
	panic("nonimplement")
}
