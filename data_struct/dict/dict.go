package dict

type dict interface {
	Put(key string, val interface{}) (result int)
	Get(key string) (val interface{}, exists bool)
}
