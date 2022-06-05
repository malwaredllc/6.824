package mr

import (
    "errors"
    "sync"
)

type SyncQueue struct {
    mu sync.RWMutex
    capacity int
    q        []interface{}
}

// FifoQueue 
type Queue interface {
    Enqueue(item interface{}) error
    Dequeue() (interface{}, error)
	Size() int
}

// Enqueue Enqueues the item into the queue
func (q *SyncQueue) Enqueue(item interface{}) error {
    q.mu.Lock()
    defer q.mu.Unlock()
    if len(q.q) < int(q.capacity) {
        q.q = append(q.q, item)
        return nil
    }
    return errors.New("Queue is full")
}

// Dequeue Dequeues the oldest element from the queue
func (q *SyncQueue) Dequeue() (interface{}, error) {
    q.mu.Lock()
    defer q.mu.Unlock()
    if len(q.q) > 0 {
        item := q.q[0]
        q.q = q.q[1:]
        return item, nil
    }
    return nil, errors.New("Queue is empty")
}

func (q *SyncQueue) Size() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.q)
}

// CreateSyncQueue creates an empty queue with desired capacity
func CreateSyncQueue(capacity int) *SyncQueue {
    return &SyncQueue{
        capacity: capacity,
        q:        make([]interface{}, 0, capacity),
    }
}
