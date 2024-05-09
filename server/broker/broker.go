package broker

import (
	"fmt"
	"go-event-queue/server/config"
	"log"
	"net"
)

type Broker interface {
	NewTopic(string) error
	Enqueue(string, string)
	Dequeue(string) (interface{}, error)
	Commit(string) error
	HandleConnection(net.Conn)
	GetQueue(string) queue
}

func RunBroker(broker Broker) {

	cfg := config.GetConfig()

	info := fmt.Sprintf(":%d", cfg.Port)

	ln, err := net.Listen("tcp", info)
	if nil != err {
		log.Println("error while opening port :", err)
	}
	defer ln.Close()
	log.Println("Broker Started!")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Failed to make connection: %v", err)
			continue
		}

		log.Printf("Client %s connected", conn.RemoteAddr())

		go broker.HandleConnection(conn)
	}
}
