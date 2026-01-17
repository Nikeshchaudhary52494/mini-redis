package persistence

import (
	"bufio"
	"os"
	"strings"
)

type AOF struct {
	file   *os.File
	writer *bufio.Writer
}

func NewAOF(path string) (*AOF, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_RDWR,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &AOF{
		file:   file,
		writer: bufio.NewWriter(file),
	}, nil
}

func (a *AOF) Append(cmd []string) error {
	line := strings.Join(cmd, " ") + "\n"
	_, err := a.writer.WriteString(line)
	if err != nil {
		return err
	}
	return a.writer.Flush()
}

func (a *AOF) Close() error {
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
