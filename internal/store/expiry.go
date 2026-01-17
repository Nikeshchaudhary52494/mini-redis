package store

import "time"

func isExpired(v Value) bool {
    if v.Expiry.IsZero() {
        return false
    }
    return time.Now().After(v.Expiry)
}
