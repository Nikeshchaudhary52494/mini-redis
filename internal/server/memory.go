package server

func (l *EventLoop) enforceMaxMemory() {
	for l.Store.ApproxSize() > l.MaxMemory {
		evicted := l.Store.EvictLRU(l.LRUSamples)
		if !evicted {
			break
		}
	}
}
