package producer

import (
	"client/config"
	"client/event"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
)

type Callback func(net.Conn) bool

type Producer struct {
	topic      string
	connection net.Conn
}

func NewProducer(topic string) *Producer {
	cfg := config.GetConfig()

	info := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	var prod = &Producer{topic: topic, connection: nil}

	conn, err := net.Dial("tcp", info)
	if err != nil {
		log.Fatalf("Failed to Connect server : %v", err)
	}

	prod.connection = conn

	log.Println("Connected to Server!")

	return prod
}

// TODO config injection
func (p *Producer) Run(done chan struct{}) {
	select {
	case <-done:
		log.Println("Received signal to stop")
		p.connection.Close()
		return
	}
}

func (p *Producer) Push(message string) error {

	if p.connection == nil {
		return errors.New("connection is closed")
	}

	evt := event.Event{
		Method:  "PUSH",
		Topic:   p.topic,
		Message: message,
	}

	err := p.write(evt)
	if err != nil {
		return fmt.Errorf("error while writing : %s", err)
	}

	log.Printf("event published : %v", message)
	return nil
}

func (p *Producer) Ping() error {

	if p.connection == nil {
		return errors.New("connection is closed")
	}

	evt := event.Event{
		Method:  "PING",
		Topic:   "",
		Message: "",
	}

	err := p.write(evt)
	if err != nil {
		return fmt.Errorf("error while writing : %s", err)
	}

	log.Printf("connection, topic alive")
	return nil
}

func (p *Producer) Close() {
	if err := p.connection.Close(); err != nil {
		fmt.Print("error close connection : ", err)
	}
}

func (p *Producer) write(evt event.Event) error {
	bEvt, err := event.SerializeEvt(evt)
	if err != nil {
		return fmt.Errorf("serialize error : %v", err)
	}

	length := uint32(len(bEvt))
	if err := binary.Write(p.connection, binary.BigEndian, length); err != nil {
		return err
	}

	if _, err := p.connection.Write(bEvt); err != nil {
		return err
	}
	return nil
}
