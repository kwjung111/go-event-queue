package broker

import (
	"container/list"
	"errors"
)

type directQueue struct {
	list *list.List
}

func NewDirectQueue() *directQueue {
	return &directQueue{
		list: list.New(),
	}
}

func (q *directQueue) enQueue(value interface{}) {
	q.list.PushBack(value)
}

func (q *directQueue) deQueue() (interface{}, error) {
	if q.list.Len() == 0 {
		return nil, errors.New("no data")
	}
	head := q.list.Front()
	q.list.Remove(head)
	return head.Value, nil
}

func (q *directQueue) len() int {
	return q.list.Len()
}

func (q *directQueue) View() []interface{} {
	result := make([]interface{}, 0, q.list.Len())

	for e := q.list.Front(); e != nil; e = e.Next() {
		result = append(result, e.Value)
	}

	return result
}
