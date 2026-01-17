package persistence

import (
	"bufio"
	"io"
	"os"
	"strings"
	"time"
)

type FsyncPolicy int

const (
	FsyncAlways FsyncPolicy = iota
	FsyncEverySec
	FsyncNo
)

type AOF struct {
	file      *os.File
	writer    *bufio.Writer
	policy    FsyncPolicy
	fsyncChan chan struct{}

	rewriting bool
	buffer    [][]string
	path      string
}


func NewAOF(path string, policy FsyncPolicy) (*AOF, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_RDWR,
		0644,
	)
	if err != nil {
		return nil, err
	}

	aof := &AOF{
		file:      file,
		writer:    bufio.NewWriter(file),
		policy:    policy,
		fsyncChan: make(chan struct{}, 1),
		path:      path,
	}

	if policy == FsyncEverySec {
		go aof.backgroundFsync()
	}

	return aof, nil
}

func (a *AOF) Append(cmd []string) error {
	line := strings.Join(cmd, " ") + "\n"

	if _, err := a.writer.WriteString(line); err != nil {
		return err
	}

	if err := a.writer.Flush(); err != nil {
		return err
	}

	if a.rewriting {
		a.buffer = append(a.buffer, cmd)
	}

	switch a.policy {
	case FsyncAlways:
		return a.file.Sync()

	case FsyncEverySec:
		select {
		case a.fsyncChan <- struct{}{}:
		default:
		}

	case FsyncNo:
		// do nothing
	}

	return nil
}

func (a *AOF) Close() error {
    _ = a.writer.Flush()
    _ = a.file.Sync()
    return a.file.Close()
}


func (a *AOF) Replay(apply func([]string)) error {
	if _, err := a.file.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(a.file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, " ")
		apply(parts)
	}

	return scanner.Err()
}


func (a *AOF) Rewrite(snapshot func(w io.Writer) error) error {
	if a.rewriting {
		return nil 
	}

	a.rewriting = true
	a.buffer = nil

	tmpPath := a.path + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	tmpWriter := bufio.NewWriter(tmpFile)

	if err := snapshot(tmpWriter); err != nil {
		return err
	}

	for _, cmd := range a.buffer {
		line := strings.Join(cmd, " ") + "\n"
		if _, err := tmpWriter.WriteString(line); err != nil {
			return err
		}
	}

	tmpWriter.Flush()
	tmpFile.Sync()
	tmpFile.Close()

	// atomic replace
	a.writer.Flush()
	a.file.Close()

	if err := os.Rename(tmpPath, a.path); err != nil {
		return err
	}

	file, err := os.OpenFile(
		a.path,
		os.O_APPEND|os.O_RDWR,
		0644,
	)
	if err != nil {
		return err
	}

	a.file = file
	a.writer = bufio.NewWriter(file)
	a.rewriting = false

	return nil
}


func (a *AOF) backgroundFsync() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = a.file.Sync()
		case <-a.fsyncChan:
			// coalesced into next tick
		}
	}
}
