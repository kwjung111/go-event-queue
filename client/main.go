package main

import (
	"client/producer"
	"fmt"
	"log"
	"sync"
	"time"
)

func main() {

	log.Printf("start")
	var wg sync.WaitGroup

	done := make(chan struct{})

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			p := producer.NewProducer("topic")
			go p.Run(done)
			defer wg.Done()
			//p.Ping()
			time.Sleep(3 * time.Second)

			err := p.Push(`{"dd":"ee"}`)
			if err != nil {
				fmt.Printf("error while publishing : %s\n", err)
				return
			}

			time.Sleep(3 * time.Second)
		}()
	}
	wg.Wait()
	close(done)
}
