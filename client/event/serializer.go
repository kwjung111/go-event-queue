package event

import (
	"bytes"
	"encoding/binary"
)

func SerializeEvt(evt Event) ([]byte, error) {
	buf := new(bytes.Buffer)

	methodlen := uint32(len(evt.Method))
	topiclen := uint32(len(evt.Topic))
	msglen := uint32(len(evt.Message))

	err := binary.Write(buf, binary.BigEndian, methodlen)
	if err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte(evt.Method)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, topiclen); err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte(evt.Topic)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msglen); err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte(evt.Message)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
func DeserializeEvent(data []byte) (Event, error) {
	buffer := bytes.NewBuffer(data)
	var event Event

	var methodlen uint32
	var topiclen uint32
	var msglen uint32

	// MethodLength, Method, TopicLength, Topic, MessageLength, Message를 순서대로 읽기
	if err := binary.Read(buffer, binary.BigEndian, &methodlen); err != nil {
		return event, err
	}
	method := make([]byte, methodlen)
	if _, err := buffer.Read(method); err != nil {
		return event, err
	}
	event.Method = string(method)

	if err := binary.Read(buffer, binary.BigEndian, &topiclen); err != nil {
		return event, err
	}
	topic := make([]byte, topiclen)
	if _, err := buffer.Read(topic); err != nil {
		return event, err
	}
	event.Topic = string(topic)

	if err := binary.Read(buffer, binary.BigEndian, &msglen); err != nil {
		return event, err
	}
	message := make([]byte, msglen)
	if _, err := buffer.Read(message); err != nil {
		return event, err
	}
	event.Message = string(message)

	return event, nil
}
