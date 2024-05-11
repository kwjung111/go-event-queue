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

		var output string
		data := queue.View()
		for _, value := range data {
			output += fmt.Sprintf("%v, ", value)
		}

		// 마지막 쉼표와 공백 제거
		output = output[:len(output)-2]

		// JSON 데이터를 클라이언트에게 반환
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(output))
	}
}

func NewQueue() {

}

func InjectBroker(b broker.Broker) {
	go func() {
		http.HandleFunc("/", brokerHandler(b))
		http.ListenAndServe(":8080", nil)
	}()
}
