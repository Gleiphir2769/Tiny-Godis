package dict

type Dict interface {
	// Put means insert or update. Insert will return 1, update will return 0
	Put(key string, val interface{}) (result int)
	Get(key string) (val interface{}, exists bool)
	Len() int
	PutIfAbsent(key string, val interface{}) (result int)
	PutIfExists(key string, val interface{}) (result int)
	Remove(key string) (result int)
}
