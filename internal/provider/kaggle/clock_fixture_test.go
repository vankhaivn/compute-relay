package kaggle

import (
	"sync"
	"time"
)

type stagingClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *stagingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}
