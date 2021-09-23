package list

// RecallFunc is used to traversal dict, if it returns false the traversal will be break
type RecallFunc func(val interface{}) bool

type List interface {
	RPush(value interface{})
	LPush(value interface{})
	LPop() interface{}
	RPop() interface{}
	Insert(index int, value interface{})
	Set(index int, value interface{})
	Len() int
	Get(index int) (val interface{})
	Range(start int, stop int) []interface{}
	ForEach(recallFunc RecallFunc)
	RemoveAllByVal(value interface{}) int
	RemoveByVal(value interface{}, count int) int
	ReverseRemove(value interface{}, count int) int
}
