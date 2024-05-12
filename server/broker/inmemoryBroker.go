package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

func (broker *InMemoryBroker) NewTopic(topic string, qType string) error {
	_, found := broker.queues[topic]
	if found {
		return errors.New("there is topic already")
	}
	switch qType {
	case "broadcast":
	case "roundrobin":
	case "direct":
		broker.queues[topic] = NewDirectQueue()
	default:
		fmt.Println("wrong type")
	}

	return nil
}

func (broker *InMemoryBroker) HandleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		var length uint32

		if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				fmt.Println("Client closed the connection")
			} else if err == net.ErrClosed {
				fmt.Println("Connection closed by the remote host")
			} else {
				fmt.Println("Error reading length :", err)
			}
			return
		}

		data := make([]byte, length)

		if _, err := readFull(conn, data); err != nil {
			fmt.Println("Error reading data :", err)
		}

		event, err := deserializeEvent(data)
		if err != nil {
			fmt.Println("Error deserializing event:", err)
			return
		}

		broker.Enqueue(event.Topic, event.Message)

		fmt.Printf("Received: %+v\n", event)
	}
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	totalRead := 0
	for totalRead < len(buf) {
		n, err := conn.Read(buf[totalRead:])
		if err != nil {
			return totalRead, err
		}
		totalRead += n
	}
	return totalRead, nil
}

func (broker *InMemoryBroker) Enqueue(topic string, message string) error {

	if !broker.HasTopic(topic) {
		return &topicNotFoundError{topic: topic}
	}

	broker.mutex.Lock()

	broker.queues[topic].enQueue(message)

	defer broker.mutex.Unlock()

	return nil
}

func (broker *InMemoryBroker) Dequeue(topic string) (interface{}, error) {

	if !broker.HasTopic(topic) {
		fmt.Printf("topic not exists: %s", topic)
		return nil, &topicNotFoundError{topic: topic}
	}

	broker.mutex.Lock()

	queue, found := broker.queues[topic]
	if !found || queue.len() == 0 {
		return "", errors.New("queue is empty : nothing to dequeue")
	}

	message, err := queue.deQueue()
	if err != nil {
		return "", errors.New("cannot dequeue")
	}

	defer broker.mutex.Unlock()

	return message, nil
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

func (broker *InMemoryBroker) HasTopic(topic string) bool {
	_, ok := broker.queues[topic]
	return ok
}

func (broker *InMemoryBroker) GetQueue(topic string) queue {
	return broker.queues[topic]
}

func (broker *InMemoryBroker) GetTopics() []string {
	broker.mutex.RLock()
	defer broker.mutex.RUnlock()

	keys := make([]string, 0, len(broker.queues))
	for key := range broker.queues {
		keys = append(keys, key)
	}

	return keys
}

func (broker *InMemoryBroker) GetEvents(topic string) ([]string, error) {
	broker.mutex.RLock()
	defer broker.mutex.RUnlock()

	if !broker.HasTopic(topic) {
		return nil, &topicNotFoundError{topic: topic}
	}

	queue := broker.queues[topic].View()
	return queue, nil
}
