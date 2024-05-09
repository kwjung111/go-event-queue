package broker

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
)

type InMemoryBroker struct {
	queues map[string]queue
	mutex  sync.RWMutex
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		queues: make(map[string]queue),
	}
}

func (broker *InMemoryBroker) NewTopic(topic string) error {
	_, found := broker.queues[topic]
	if found {
		return errors.New("there is topic already")
	}

	broker.queues[topic] = NewQueue()

	return nil
}

func (broker *InMemoryBroker) HandleConnection(conn net.Conn) {
	defer conn.Close()

	log.Println("Waiting for events...")
	buffer := make([]byte, 1024)
	for {
		count, _ := conn.Read(buffer)
		//conn.Write(buffer[:count])
		if count > 0 {
			evt := "evt"
			broker.Enqueue("topic", evt)
			log.Printf(fmt.Sprintf("%s", buffer[:count]))
		}
	}
}

func (broker *InMemoryBroker) Enqueue(topic string, event string) {
	broker.mutex.Lock()

	broker.queues[topic].enQueue(event)

	defer broker.mutex.Unlock()
}

func (broker *InMemoryBroker) Dequeue(topic string) (interface{}, error) {
	broker.mutex.Lock()

	queue, found := broker.queues[topic]
	if !found || queue.len() == 0 {
		return "", errors.New("queue is empty : nothing to dequeue")
	}

	event, err := queue.deQueue()
	if err != nil {
		return "", errors.New("cannot dequeue")
	}

	defer broker.mutex.Unlock()

	return event, nil
}

func (broker *InMemoryBroker) Commit(topic string) error {
	broker.mutex.Lock()

	queue, found := broker.queues[topic]
	if !found || queue.len() == 0 {
		return errors.New("queue is empty : nothing to commit")
	}

	//commit

	defer broker.mutex.Lock()
	return nil
}

func (broker *InMemoryBroker) GetQueue(topic string) queue {
	return broker.queues[topic]
}
