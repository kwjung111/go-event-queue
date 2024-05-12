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
	q.list.PushBack(value.(string))
}

func (q *directQueue) deQueue() (string, error) {
	if q.list.Len() == 0 {
		return "", errors.New("no data")
	}
	head := q.list.Front()
	q.list.Remove(head)
	return head.Value.(string), nil
}

func (q *directQueue) len() int {
	return q.list.Len()
}

func (q *directQueue) View() []string {
	result := make([]string, 0, q.list.Len())

	for e := q.list.Front(); e != nil; e = e.Next() {
		result = append(result, e.Value.(string))
	}

	return result
}
