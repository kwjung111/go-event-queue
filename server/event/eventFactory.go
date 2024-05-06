package event

import "sync"

type EventFactory struct{}

var (
	once     sync.Once
	instance *EventFactory
)

func (f *EventFactory) New(topic string, event string) Event {
	return &JsonEvent{topic: topic, event: event}
}

func GetEventFactory() *EventFactory {
	once.Do(func() {
		instance = &EventFactory{}
	})
	return instance
}
