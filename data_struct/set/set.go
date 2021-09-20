package set

import "Tiny-Godis/data_struct/dict"

type Set struct {
	d dict.Dict
}

func MakeSet(members ...string) *Set {
	s := Set{d: dict.MakeSimpleDict()}
	for _, m := range members {
		s.Add(m)
	}
	return &s
}

func (s *Set) Add(val string) int {
	return s.d.Put(val, struct{}{})
}

func (s *Set) Remove(val string) int {
	return s.d.Remove(val)
}

func (s *Set) Has(val string) bool {
	_, ok := s.d.Get(val)
	return ok
}

func (s *Set) Len() int {
	return s.d.Len()
}

func (s *Set) ToSlice() []string {
	slice := make([]string, 0)
	s.ForEach(func(key string, val interface{}) bool {
		slice = append(slice, key)
		return true
	})
	return slice
}

func (s *Set) ForEach(recall dict.RecallFunc) {
	s.d.ForEach(recall)
}

func (s *Set) Intersect(another *Set) *Set {
	result := MakeSet()
	s.ForEach(func(key string, val interface{}) bool {
		if another.Has(key) {
			result.Add(key)
		}
		return true
	})
	return result
}

func (s *Set) Union(another *Set) *Set {
	result := MakeSet()
	s.ForEach(func(key string, val interface{}) bool {
		result.Add(key)
		return true
	})
	another.ForEach(func(key string, val interface{}) bool {
		result.Add(key)
		return true
	})
	return result
}

func (s *Set) Diff(another *Set) *Set {
	result := MakeSet()
	s.ForEach(func(key string, val interface{}) bool {
		if !another.Has(key) {
			result.Add(key)
		}
		return true
	})
	return result
}
