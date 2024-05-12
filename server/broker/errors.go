package broker

import "fmt"

type topicNotFoundError struct {
	topic string
}

func (e *topicNotFoundError) Error() string {
	return fmt.Sprintf("topic %s not exists", e.topic)
}
