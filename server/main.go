package main

import (
	"go-event-queue/server/api"
	"go-event-queue/server/broker"
	"sync"
)

func main() {

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {

		defer wg.Done()
		b := broker.NewInMemoryBroker()
		api.InjectBroker(b)
		api.RunApiServer()
		//b.NewTopic("topic", "direct")
		broker.RunBroker(b)
	}()

	wg.Wait()
}
