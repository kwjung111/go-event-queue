package broker

type queue interface {
	enQueue(interface{})
	deQueue() (interface{}, error)
	len() int
	View() []interface{}
}
