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

func getTopic(w http.ResponseWriter, r *http.Request, b broker.Broker) {
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

func setNewDirectTopic(w http.ResponseWriter, r *http.Request, b broker.Broker) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var jsonData map[string]interface{}
	err := json.NewDecoder(r.Body).Decode(&jsonData)
	if err != nil {
		http.Error(w, "Failed to parse json data", http.StatusInternalServerError)
	}

	topic, ok := jsonData["topic"].(string)
	if !ok {
		http.Error(w, "no \"topic\" filed ", http.StatusBadRequest)
		return
	}

	if topic == "" {
		http.Error(w, "Topic cannot be empty", http.StatusBadRequest)
		return
	}

	b.NewTopic(topic, "Direct")

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "New Topic has Created : %s", topic)
}

func topicHandler(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getTopic(w, r, b)
		case http.MethodPost:
			setNewDirectTopic(w, r, b)
		default:
			notAllowedMethod(w)
		}
	}
}

func notAllowedMethod(w http.ResponseWriter) {
	http.Error(w, "Method not Allowed", http.StatusMethodNotAllowed)
}

func InjectBroker(b broker.Broker) {
	http.HandleFunc("/topic", topicHandler(b))
	http.HandleFunc("/event/", getEvents(b))
}
func RunApiServer() {
	go func() {
		http.ListenAndServe(":8080", nil)
	}()
}
