package broker

import (
	"bytes"
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

func (broker *InMemoryBroker) NewTopic(topic string) error {
	_, found := broker.queues[topic]
	if found {
		return errors.New("there is topic already")
	}

	broker.queues[topic] = NewDirectQueue()

	return nil
}

func (broker *InMemoryBroker) HandleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		var length uint32

		if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				fmt.Println("Client closed the connection")
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

func deserializeEvent(data []byte) (Event, error) {
	buffer := bytes.NewBuffer(data)
	var event Event

	var methodlen uint32
	var topiclen uint32
	var msglen uint32

	// MethodLength, Method, TopicLength, Topic, MessageLength, Message를 순서대로 읽기
	if err := binary.Read(buffer, binary.BigEndian, &methodlen); err != nil {
		return event, err
	}
	method := make([]byte, methodlen)
	if _, err := buffer.Read(method); err != nil {
		return event, err
	}
	event.Method = string(method)

	if err := binary.Read(buffer, binary.BigEndian, &topiclen); err != nil {
		return event, err
	}
	topic := make([]byte, topiclen)
	if _, err := buffer.Read(topic); err != nil {
		return event, err
	}
	event.Topic = string(topic)

	if err := binary.Read(buffer, binary.BigEndian, &msglen); err != nil {
		return event, err
	}
	message := make([]byte, msglen)
	if _, err := buffer.Read(message); err != nil {
		return event, err
	}
	event.Message = string(message)

	return event, nil
}
