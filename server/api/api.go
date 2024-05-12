package api

import (
	"encoding/json"
	"fmt"
	"go-event-queue/server/broker"
	"net/http"
	"strings"
)

func init() {

}

func ErrorResponse(w http.ResponseWriter, err error, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	fmt.Fprintf(w, `{"error": "%s"}`, err.Error())
}

func getEvents(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		topic := parts[len(parts)-1]
		events, err := b.GetEvents(topic)
		if err != nil {
			ErrorResponse(w, err, http.StatusInternalServerError)
			return
		}
		for _, event := range events {
			fmt.Printf(event)
		}

		eventsJSON, err := json.Marshal(events)
		if err != nil {
			fmt.Printf("error convert json : %s", err)
			http.Error(w, "Faild to convert json", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(eventsJSON)
	}
}

func getTopics(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topics := b.GetTopics()

		topicsJSON, err := json.Marshal(topics)
		if err != nil {
			fmt.Printf("error convert json : %s", err)
			http.Error(w, "Faild to convert json", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(topicsJSON)
	}
}

func InjectBroker(b broker.Broker) {
	http.HandleFunc("/topics", getTopics(b))
	http.HandleFunc("/events/", getEvents(b))
}
func RunApiServer() {
	go func() {
		http.ListenAndServe(":8080", nil)
	}()
}
