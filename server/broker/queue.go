package broker

type queue interface {
	enQueue(interface{})
	deQueue() (string, error)
	len() int
	view() []string
}
