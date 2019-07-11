package mqwrap

import (
	"sync"
)

/*
	Use a semaphore to limit access to a resource.

	Example:
		* No more than 100 database connections.
		* No more than 10 file writers.

	If the semaphore is maxed out, Up() will block until the
	semaphore releases via Down() in another thread.

	Dynamically adjust the semaphore max with SetMax(newmax).
	  * If newmax is higher, then waiting threads will be signaled up to newmax.
	  * If newmax is lower, then threads will continue to run until complete,
	    but no new threads signaled until the count is below new max.

	// To limit 100 connections to DB
	s := NewSemaphore(100)

	...

	go func(s *Semaphore) {
		s.Up()
		db.Connect()

		...

		db.Disconnect()
		db.Down()
	}(s)
*/

type Semaphore struct {
	mutex   *sync.Mutex
	cond    *sync.Cond
	max     int
	current int
}

func NewSemaphore(max int) *Semaphore {
	mutex := &sync.Mutex{}
	cond := sync.NewCond(mutex)
	return &Semaphore{mutex, cond, max, 0}
}

// Set new max and signal if greater than old max
func (s *Semaphore) SetMax(newMax int) {
	if newMax > 0 {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		for i := s.max; i < newMax; i++ {
			s.cond.Signal()
		}
		s.max = newMax
	}
}

func (s *Semaphore) Current() int {
	var current int
	s.mutex.Lock()
	current = s.current
	s.mutex.Unlock()
	return current
}

func (s *Semaphore) Up() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if (s.current + 1) > s.max {
		s.cond.Wait()
	}
	s.current = s.current + 1
}

func (s *Semaphore) Down() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if (s.current - 1) >= 0 {
		s.current = s.current - 1
		s.cond.Signal()

	}
}
