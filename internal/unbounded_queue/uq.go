// unbounded queue lib

package unboundedqueue

import (
	"errors"
	"sync"
)

type UnboundedQueue struct {
	m      sync.Mutex
	c      *sync.Cond
	closed bool
	q      []interface{}
}

func NewUnboundedQueue() *UnboundedQueue {
	q := &UnboundedQueue{}
	q.c = sync.NewCond(&q.m)
	q.q = make([]interface{}, 0)
	return q
}

func (uq *UnboundedQueue) Push(i interface{}) error {
	uq.m.Lock()
	if uq.closed {
		uq.m.Unlock()
		return errors.New("closed")
	}
	uq.q = append(uq.q, i)
	uq.m.Unlock()
	uq.c.Broadcast()
	return nil
}

func (uq *UnboundedQueue) PopAll() ([]interface{}, error) {
	for {
		uq.m.Lock()
		if len(uq.q) == 0 {
			if uq.closed {
				uq.c.L.Unlock()
				return nil, errors.New("closed")
			}
			uq.c.Wait()
			uq.c.L.Unlock()
			continue
		} else {
			tmp := uq.q
			uq.q = make([]interface{}, 0)
			uq.m.Unlock()
			return tmp, nil
		}
	}
}

func (uq *UnboundedQueue) Close() {
	uq.m.Lock()
	uq.closed = true
	uq.m.Unlock()
	uq.c.Broadcast()
}
