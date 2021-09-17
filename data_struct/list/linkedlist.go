package list

import (
	"Tiny-Godis/lib/utils"
	"container/list"
)

type LinkedList struct {
	l *list.List
}

func MakeLinkedList() *LinkedList {
	ll := LinkedList{l: list.New()}
	return &ll
}

func (ll *LinkedList) LPush(value interface{}) {
	ll.l.PushFront(value)
}

func (ll *LinkedList) RPush(value interface{}) {
	ll.l.PushBack(value)
}

func (ll *LinkedList) find(index int) *list.Element {
	ele := ll.l.Front()
	back := true
	if index >= ll.l.Len() || index < 0 {
		return nil
	}
	if index > ll.l.Len()/2 {
		ele = ll.l.Back()
		back = false
		index = ll.l.Len() - 1 - index
	}
	for i := index; i > 0; i-- {
		if !back {
			ele = ele.Prev()
		}
		ele = ele.Next()
	}
	return ele
}

func (ll *LinkedList) Insert(index int, value interface{}) {
	ele := ll.find(index)
	if ele == nil {
		return
	}
	ll.l.InsertBefore(value, ele)
}

func (ll *LinkedList) LPop() interface{} {
	e := ll.l.Front()
	ll.l.Remove(e)
	return e.Value
}

func (ll *LinkedList) RPop() interface{} {
	e := ll.l.Back()
	ll.l.Remove(e)
	return e.Value
}

func (ll *LinkedList) Len() int {
	return ll.l.Len()
}

func (ll *LinkedList) RemoveAllByVal(value interface{}) int {
	ele := ll.l.Front()
	removed := 0
	for i := 0; i < ll.Len(); i++ {
		if utils.Equals(ele.Value, value) {
			temp := ele
			ele = ele.Next()
			ll.l.Remove(temp)
			removed++
		}
	}
	return removed
}

func (ll *LinkedList) RemoveByVal(value interface{}, count int) int {
	if count <= 0 {
		return 0
	}
	removed := 0
	ele := ll.l.Front()
	for i := 0; i < ll.Len(); i++ {
		if utils.Equals(ele.Value, value) {
			temp := ele
			ele = ele.Next()
			ll.l.Remove(temp)
			removed++
			if removed >= count {
				break
			}
		}
	}
	return removed
}

func (ll *LinkedList) ReverseRemove(value interface{}, count int) int {
	if count >= 0 {
		return 0
	}
	count = -count
	removed := 0
	ele := ll.l.Back()
	for i := 0; i < ll.Len(); i++ {
		if utils.Equals(ele.Value, value) {
			temp := ele
			ele = ele.Prev()
			ll.l.Remove(temp)
			removed++
			if removed >= count {
				break
			}
		}
	}
	return removed
}

func (ll *LinkedList) Get(index int) (val interface{}) {
	if ll == nil {
		panic("list is nil")
	}
	if index < 0 || index >= ll.Len() {
		panic("index out of bound")
	}
	return ll.find(index).Value
}

func (ll *LinkedList) Range(start int, stop int) []interface{} {
	if start >= stop || start < 0 || stop >= ll.Len() {
		return nil
	}
	rangeList := make([]interface{}, stop-start)
	se := ll.find(start)
	for i := start; i < stop; i++ {
		rangeList = append(rangeList, se)
		se = se.Next()
	}
	return rangeList
}
