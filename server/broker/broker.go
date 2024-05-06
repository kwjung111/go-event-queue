package broker

import (
	"fmt"
	"go-event-queue/server/config"
	"io"
	"log"
	"net"
)

type Broker interface {
	Enqueue(topic string, message string)
	Dequeue(topic string) (string, error)
	Commit(topic string) error
}

func RunBroker(broker Broker) {

	cfg := config.GetConfig()

	info := fmt.Sprintf(":%d", cfg.Port)

	fmt.Println(info)

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

		handleConnection(conn, broker)
	}
}

func handleConnection(conn net.Conn, broker Broker) {
	defer conn.Close()

	// 데이터를 읽고 이벤트 처리하는 고루틴
	go func() {
		// 연결이 닫힌 경우를 확인하여 루프를 종료
		for {
			// 클라이언트로부터 데이터를 읽어옴
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				if err != io.EOF {
					log.Println("Error while reading data:", err)
				}
				break // 연결이 닫혔을 때 루프 종료
			}

			// 받은 데이터를 이벤트로 처리
			event := string(buf[:n])
			broker.Enqueue("topic", event)

			log.Println("Received event:", event)
		}
	}()

	// 다른 작업 수행 가능
	log.Println("Waiting for events...")
	// 이곳에서 다른 작업을 수행하거나 대기할 수 있습니다.
}
