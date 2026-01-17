package server

import "time"

func (l *EventLoop) runActiveExpiry() {
	const sampleSize = 20
	const maxDeletes = 25

	keys := l.Store.RandomKeysWithTTL(sampleSize)
	deleted := 0

	for _, key := range keys {
		if l.Store.DeleteIfExpired(key) {
			deleted++
			if deleted >= maxDeletes {
				break
			}
		}
	}
}

func StartExpiryTicker(loop *EventLoop) {
	ticker := time.NewTicker(100 * time.Millisecond)

	go func() {
		for range ticker.C {
			loop.Commands <- Command{
				Type: InternalExpireCommand,
			}
		}
	}()
}
