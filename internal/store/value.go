package store

import "time"

type ValueType string

const (
	StringType ValueType = "string"
)

type Value struct {
	Type       ValueType
	Data       string
	Expiry     time.Time // zero value means no expiry
	LastAccess int64
}
