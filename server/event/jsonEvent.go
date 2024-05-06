package event

type JsonEvent struct {
	topic string
	event string
}

func NewJSonEvent(topic string, event string) JsonEvent {
	jsonEvent := JsonEvent{topic, event}

	return jsonEvent
}

func (j *JsonEvent) GetEvent() string {
	return j.event
	/*
		jsonBytes, err := json.Marshal(&j.Content)
		if err != nil {
			log.Println("err while encoding json :", err)
		}
		jsonString := string(jsonBytes)
		return jsonString
	*/
}

func (j *JsonEvent) GetTopic() string {
	return j.topic
}
