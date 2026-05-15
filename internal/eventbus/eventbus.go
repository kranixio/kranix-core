package eventbus

import (
	"context"
	"sync"

	"github.com/kranix-io/kranix-core/pkg/types"
)

// EventBus provides a typed event system for internal communication.
type EventBus struct {
	subscribers map[types.EventType][]chan types.Event
	bufferSize  int
	mu          sync.RWMutex
}

// New creates a new EventBus with the specified buffer size.
func New(bufferSize int) *EventBus {
	return &EventBus{
		subscribers: make(map[types.EventType][]chan types.Event),
		bufferSize:  bufferSize,
	}
}

// Subscribe registers a channel to receive events of the specified type.
func (eb *EventBus) Subscribe(eventType types.EventType) <-chan types.Event {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	ch := make(chan types.Event, eb.bufferSize)
	eb.subscribers[eventType] = append(eb.subscribers[eventType], ch)
	return ch
}

// Unsubscribe removes a channel from the subscriber list.
func (eb *EventBus) Unsubscribe(eventType types.EventType, ch <-chan types.Event) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	channels := eb.subscribers[eventType]
	for i, c := range channels {
		if c == ch {
			eb.subscribers[eventType] = append(channels[:i], channels[i+1:]...)
			close(c)
			break
		}
	}
}

// Publish sends an event to all subscribers of its type.
func (eb *EventBus) Publish(ctx context.Context, event types.Event) error {
	eb.mu.RLock()
	channels := eb.subscribers[event.Type]
	eb.mu.RUnlock()

	for _, ch := range channels {
		select {
		case ch <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// PublishAsync publishes an event without blocking.
func (eb *EventBus) PublishAsync(event types.Event) {
	eb.mu.RLock()
	channels := eb.subscribers[event.Type]
	eb.mu.RUnlock()

	for _, ch := range channels {
		select {
		case ch <- event:
		default:
			// Buffer full, drop event
		}
	}
}
