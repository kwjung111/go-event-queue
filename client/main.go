package main

import (
	"go-event-queue/client/client"
	"log"
	"sync"
	"time"
)

func main() {

	log.Printf("client start")
	var wg sync.WaitGroup

	done := make(chan struct{})

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			c := client.NewClient("topic")
			go c.Run(done)
			time.Sleep(3 * time.Second)
			c.Pub("dfd")
			time.Sleep(10 * time.Second)
			defer wg.Done()
		}()
	}

	time.Sleep(5000 * time.Second)

	close(done)

	wg.Wait()
}
