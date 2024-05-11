package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"go-event-queue/client/config"
	"log"
	"net"
)

type Callback func(net.Conn) bool

type Client struct {
	topic      string
	connection net.Conn
}

type Event struct {
	Method  string
	Topic   string
	Message string
}

func NewClient(topic string) *Client {
	return &Client{topic: topic, connection: nil}
}

// TODO config injection
func (c *Client) Run(done chan struct{}) {

	cfg := config.GetConfig()

	info := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	conn, err := net.Dial("tcp", info)
	if err != nil {
		log.Fatalf("Failed to Connect server : %v", err)
	}

	c.connection = conn

	log.Println("Connected to Server!")

	select {
	case <-done:
		log.Println("Received signal to stop")
	}
}

func (c *Client) Push(message string) {

	if c.connection == nil {
		log.Println("Connection is closed")
		return
	}

	evt := Event{
		Method:  "PUSH",
		Topic:   c.topic,
		Message: message,
	}

	bEvt, err := serializeEvt(evt)
	if err != nil {
		fmt.Printf("serialize error : %v", err)
	}

	length := uint32(len(bEvt))
	if err := binary.Write(c.connection, binary.BigEndian, length); err != nil {
		fmt.Println("error writing length :", err)
		return
	}

	if _, err := c.connection.Write(bEvt); err != nil {
		fmt.Println("error writing data :", err)
	}

	log.Printf("event published : %v", message)
}

// TODO implement
func (c *Client) Pull() (Event, error) {

	if c.connection == nil {
		log.Println("Connection closed")
		return Event{}, errors.New("connnection error")
	}

	return Event{}, nil

}

func (c *Client) Close() {
	if err := c.connection.Close(); err != nil {
		fmt.Print("error close connection : ", err)
	}
}

func serializeEvt(evt Event) ([]byte, error) {
	buf := new(bytes.Buffer)

	methodlen := uint32(len(evt.Method))
	topiclen := uint32(len(evt.Topic))
	msglen := uint32(len(evt.Message))

	err := binary.Write(buf, binary.BigEndian, methodlen)
	if err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte(evt.Method)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, topiclen); err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte(evt.Topic)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msglen); err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte(evt.Message)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
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
