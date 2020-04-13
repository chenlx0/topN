package internal

// Msg represents each single string
type Msg struct {
	data   []byte
	hash   []byte
	offset int64
	occurs int
}
