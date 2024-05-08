package broker

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
)

type InMemoryBroker struct {
	queues map[string][]string
	mutex  sync.RWMutex
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		queues: make(map[string][]string),
	}
}

func (broker *InMemoryBroker) HandleConnection(conn net.Conn) {
	defer conn.Close()

	log.Println("Waiting for events...")
	buffer := make([]byte, 1024)
	for {
		count, _ := conn.Read(buffer)
		//conn.Write(buffer[:count])
		if count > 0 {
			//parse topic
			broker.Enqueue("topic", string(buffer))
			log.Printf(fmt.Sprintf("%s", buffer))
		}
	}
}

func (broker *InMemoryBroker) Enqueue(topic string, event string) {
	broker.mutex.Lock()

	broker.queues[topic] = append(broker.queues[topic], event)

	defer broker.mutex.Unlock()
}

func (broker *InMemoryBroker) Dequeue(topic string) (string, error) {
	broker.mutex.Lock()

	queue, found := broker.queues[topic]
	if !found || len(queue) == 0 {
		return "", errors.New("queue is empty : nothing to dequeue")
	}

	event := queue[0]

	defer broker.mutex.Unlock()

	return event, nil
}

func (broker *InMemoryBroker) Commit(topic string) error {
	broker.mutex.Lock()

	queue, found := broker.queues[topic]
	if !found || len(queue) == 0 {
		return errors.New("queue is empty : nothing to commit")
	}

	broker.queues[topic] = queue[1:]

	defer broker.mutex.Lock()
	return nil
}

func (broker *InMemoryBroker) GetQueue(topic string) []string {
	return broker.queues[topic]
}
