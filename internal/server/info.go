package server

import (
	"strconv"
	"strings"
	"time"
)

func (l *EventLoop) buildInfo(section string) string {
	var b strings.Builder

	uptime := int64(time.Since(l.StartTime).Seconds())

	if section == "all" || section == "server" {
		b.WriteString("# Server\n")
		b.WriteString("uptime_in_seconds:")
		b.WriteString(strconv.FormatInt(uptime, 10))
		b.WriteString("\n\n")
	}

	if section == "all" || section == "memory" {
		b.WriteString("# Memory\n")
		b.WriteString("used_memory:")
		b.WriteString(strconv.FormatInt(l.Store.ApproxSize(), 10))
		b.WriteString("\n")

		b.WriteString("maxmemory:")
		b.WriteString(strconv.FormatInt(l.MaxMemory, 10))
		b.WriteString("\n\n")
	}

	if section == "all" || section == "stats" {
		b.WriteString("# Stats\n")
		b.WriteString("total_commands_processed:")
		b.WriteString(strconv.FormatInt(l.CommandsSeen, 10))
		b.WriteString("\n\n")
	}

	if section == "all" || section == "persistence" {
		b.WriteString("# Persistence\n")
		if l.AOF != nil {
			b.WriteString("aof_enabled:1\n")
		} else {
			b.WriteString("aof_enabled:0\n")
		}
		b.WriteString("\n")
	}

	return b.String()
}
