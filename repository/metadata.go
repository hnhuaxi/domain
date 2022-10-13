package repository

type SearchMetadata struct {
	Count    int
	Total    int
	Page     int
	PageSize int
	PrevID   string
	NextID   string
}
