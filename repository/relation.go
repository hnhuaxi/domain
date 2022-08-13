package repository

type RelationDef struct {
	Join      bool
	Query     string
	ArgsCount int
}
