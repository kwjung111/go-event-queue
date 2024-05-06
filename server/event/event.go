package event

type Event interface {
	GetEvent() string
	GetTopic() string
}
