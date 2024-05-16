package broker

import (
	"container/list"
	"errors"
)

type roundRobinQueue struct {
	listener []*directQueue
	list     *list.List
}

func NewRoundRobinQueue() *roundRobinQueue {
	return &roundRobinQueue{
		listener: make([]*directQueue, 0),
		list:     list.New(),
	}
}

func (r *roundRobinQueue) AddListener(q *directQueue) {
	r.listener = append(r.listener, q)
}

func (r *roundRobinQueue) enQueue(value interface{}) {
	r.list.PushBack(value.(string))
}

func (r *roundRobinQueue) deQueue() (string, error) {
	if r.list.Len() == 0 {
		return "", errors.New("no data")
	}
	head := r.list.Front()
	r.list.Remove(head)
	return head.Value.(string), nil
}

func (r *roundRobinQueue) len() int {
	return r.list.Len()
}

func (r *roundRobinQueue) view() []string {
	result := make([]string, 0, r.list.Len())

	for e := r.list.Front(); e != nil; e = e.Next() {
		result = append(result, e.Value.(string))
	}

	return result
}

func run() {
	go func() {

		select {}

	}()
}
