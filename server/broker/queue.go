package broker

import (
	"container/list"
	"errors"
)

type simpleQueue struct {
	list *list.List
}

type queue interface {
	enQueue(interface{})
	deQueue() (interface{}, error)
	len() int
	View() []interface{}
}

func NewQueue() *simpleQueue {
	return &simpleQueue{
		list: list.New(),
	}
}

func (q *simpleQueue) enQueue(value interface{}) {
	q.list.PushBack(value)
}

func (q *simpleQueue) deQueue() (interface{}, error) {
	if q.list.Len() == 0 {
		return nil, errors.New("no data")
	}
	head := q.list.Front()
	q.list.Remove(head)
	return head.Value, nil
}

func (q *simpleQueue) len() int {
	return q.list.Len()
}

func (q *simpleQueue) View() []interface{} {
	result := make([]interface{}, 0, q.list.Len())

	for e := q.list.Front(); e != nil; e = e.Next() {
		result = append(result, e.Value)
	}

	return result
}
