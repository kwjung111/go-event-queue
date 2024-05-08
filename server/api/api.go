package api

import (
	"fmt"
	"go-event-queue/server/broker"
	"net/http"
)

func init() {

}

func brokerHandler(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		queue := b.GetQueue("topic")
		for _, msg := range queue {
			fmt.Fprintf(w, "%s ,", msg)
		}
	}
}

func InjectBroker(b broker.Broker) {
	go func() {
		http.HandleFunc("/", brokerHandler(b))
		http.ListenAndServe(":8080", nil)
	}()
}
