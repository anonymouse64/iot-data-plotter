package main

// most of this code is adapted from https://stackoverflow.com/a/49877632/10102404

// Broker is an object for maintaining subscriptions such that multiple
// clients can simultaneously listen for messages on a channel safely
type Broker struct {
	stopCh    chan struct{}
	publishCh chan interface{}
	subCh     chan chan interface{}
	unsubCh   chan chan interface{}
}

// NewBroker returns a pointer to a new broker initialized with suitable
// internal channels
func NewBroker() *Broker {
	return &Broker{
		stopCh:    make(chan struct{}),
		publishCh: make(chan interface{}, 1),
		subCh:     make(chan chan interface{}, 1),
		unsubCh:   make(chan chan interface{}, 1),
	}
}

// Start starts the broker listening - note this should be run inside it's own
// go routine
func (b *Broker) Start() {
	subs := map[chan interface{}]struct{}{}
	for {
		select {
		case <-b.stopCh:
			return
		case msgCh := <-b.subCh:
			subs[msgCh] = struct{}{}
		case msgCh := <-b.unsubCh:
			delete(subs, msgCh)
		case msg := <-b.publishCh:
			for msgCh := range subs {
				// msgCh is buffered, use non-blocking send to protect the broker:
				select {
				case msgCh <- msg:
				default:
				}
			}
		}
	}
}

// Stop closes all channels and effectively cancels all subscriptions
func (b *Broker) Stop() {
	close(b.stopCh)
}

// Subscribe returns a channel that a single client can use to listen to all
// messages published
func (b *Broker) Subscribe() chan interface{} {
	msgCh := make(chan interface{}, 5)
	b.subCh <- msgCh
	return msgCh
}

// Publish sends a new message to be delivered to all subscribers
func (b *Broker) Publish(msg interface{}) {
	b.publishCh <- msg
}

// Unsubscribe closes and deletes a channel that was subscribed
func (b *Broker) Unsubscribe(msgCh chan interface{}) {
	b.unsubCh <- msgCh
	close(msgCh)
}
