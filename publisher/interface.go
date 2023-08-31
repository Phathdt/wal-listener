package publisher

type Publisher interface {
	Publish(topic string, event Event) error
}
