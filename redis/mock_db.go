package redis

type MockDB struct {
	kv map[string]interface{}
}

func NewMockDB() *MockDB {
	return &MockDB{}
}