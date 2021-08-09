package crawler

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

type FileLogger struct {
	name string

	mu     sync.Mutex
	f      *os.File
	writer *bufio.Writer
}

const DEFAULT_PERIOD = 60

func NewFileLogger(name string) (*FileLogger, error) {

	fName := getFileName(name, DEFAULT_PERIOD)

	file, err := os.Create(fName)
	if err != nil {
		return nil, fmt.Errorf("error creating new file %s: %v", fName, err)
	}

	return &FileLogger{
		name:   fName,
		f:      file,
		writer: bufio.NewWriter(file),
	}, nil
}

func (logger *FileLogger) Write(s string) {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	// Add current time to log
	toWrite := fmt.Sprintf("%d,%s\n", time.Now().Unix(), s)

	_, err := logger.writer.WriteString(toWrite)
	if err != nil {
		fmt.Printf("Error writing to log file: %v", err)
	}
}

func (logger *FileLogger) Writef(format string, a ...interface{}) {
	logger.Write(fmt.Sprintf(format, a...))
}

func (logger *FileLogger) Close() error {
	err := logger.writer.Flush()
	if err != nil {
		return fmt.Errorf("error flushing logger: %v", err)
	}

	return logger.f.Close()
}

func getFileName(base string, period int64) string {
	// Create timestamp for file, mod write period
	timestamp := (time.Now().UTC().Unix() / period) * period
	// Format timestamp
	tsFormat := time.Unix(timestamp, 0).UTC()
	// Create full timestamp string:
	tsString := fmt.Sprintf("%04d_%02d_%02d__%02d_%02d_%02d", tsFormat.Year(), int(tsFormat.Month()), tsFormat.Day(),
		tsFormat.Hour(), tsFormat.Minute(), tsFormat.Second())

	return fmt.Sprintf("%s_%s.txt", base, tsString)
}
