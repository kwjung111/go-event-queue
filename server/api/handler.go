package api

import (
	"go-event-queue/server/broker"
	"net/http"
)

func testHandler(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			dequeueTestFunc(w, r, b)
		default:
		}
	}

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
