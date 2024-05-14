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
	case "Broadcast":
	case "Roundrobin":
	case "Direct":
		broker.queues[topic] = NewDirectQueue()
	default:
		fmt.Printf("wrong topic type : %s", qType)
	}

	fmt.Printf("New Topic Created : %s", topic)

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
				fmt.Printf("Error reading length : %s\n", err)
			}
			//connection end
			return
		}

		data := make([]byte, length)

		if _, err := readFull(conn, data); err != nil {
			fmt.Printf("client %s => Error reading data : %s\n", conn.RemoteAddr(), err)
			continue
		}

		event, err := deserializeEvent(data)
		if err != nil {
			fmt.Printf("client %s => Error deserializing event: %s\n", conn.RemoteAddr(), err)
			continue
		}

		errEnq := broker.Enqueue(event.Topic, event.Message)
		if errEnq != nil {
			fmt.Printf("client %s => Error while Enqueue : %s\n", conn.RemoteAddr(), errEnq)
			continue
		}
		fmt.Printf("client %s => Received: %+v\n", conn.RemoteAddr(), event)
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

func (broker *InMemoryBroker) Dequeue(topic string) (string, error) {

	if !broker.HasTopic(topic) {
		return "", &topicNotFoundError{topic: topic}
	}

	broker.mutex.Lock()
	defer broker.mutex.Unlock()

	queue, found := broker.queues[topic]
	if !found || queue.len() == 0 {
		return "", errors.New("queue is empty : nothing to dequeue")
	}

	message, err := queue.deQueue()
	if err != nil {
		return "", errors.New("cannot dequeue")
	}

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
