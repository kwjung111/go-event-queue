package consumer

import (
	"client/event"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
)

type consumer struct {
	topic      string
	connection net.Conn
}

// TODO implement
func (c *consumer) Pull() error {

	if c.connection == nil {
		log.Println("Connection closed")
		return errors.New("connnection error")
	}

	evt := event.Event{
		Method:  "PULL",
		Topic:   c.topic,
		Message: "",
	}

	err := c.write(evt)
	if err != nil {
		return err
	}
	return nil
}

func (c *consumer) write(evt event.Event) error {
	bEvt, err := event.SerializeEvt(evt)
	if err != nil {
		return fmt.Errorf("serialize error : %v", err)
	}

	length := uint32(len(bEvt))
	if err := binary.Write(c.connection, binary.BigEndian, length); err != nil {
		return err
	}

	if _, err := c.connection.Write(bEvt); err != nil {
		return err
	}
	return nil
}

func (c *consumer) Init() {

}

func (c *consumer) Run() {
	for {

	}
}
