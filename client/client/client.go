package client

import (
	"encoding/json"
	"fmt"
	"go-event-queue/client/config"
	"log"
	"net"
)

type ClientFunctions interface {
	Run(chan struct{})
	Poll()
	Pub()
}

type Client struct {
	topic      string
	connection net.Conn
}

func NewClient(topic string) *Client {
	return &Client{topic: topic, connection: nil}
}

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

func (c *Client) Pub(evtStr string) {

	if c.connection == nil {
		log.Println("Connection is closed")
		return
	}

	data := map[string]string{
		"topic": c.topic,
		"event": evtStr,
	}

	json, err := json.Marshal(data)
	if err != nil {
		log.Printf("Failed to Convert event : %v", err)
		return
	}

	_, err2 := c.connection.Write(json)
	if err2 != nil {
		log.Printf("Failed to send Message : %v", err2)
	}

	log.Printf("event publisehd : %v", data)
}

func (c *Client) Poll() {

}
