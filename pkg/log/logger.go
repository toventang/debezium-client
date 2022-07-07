package log

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sync"
	"syscall"
)

const logFileName = "debecli.log"

type FileLogger struct {
	file     *os.File
	filename string
	channel  chan []byte
	wg       sync.WaitGroup
	done     chan struct{}
}

func NewFileLogger(fp string) (io.WriteCloser, error) {
	logger := &FileLogger{
		filename: path.Join(fp, logFileName),
		channel:  make(chan []byte, 1),
		done:     make(chan struct{}),
	}
	if err := logger.createLogFile(); err != nil {
		return nil, err
	}
	logger.start()
	return logger, nil
}

func (l *FileLogger) Write(p []byte) (n int, err error) {
	select {
	case l.channel <- p:
		return len(p), nil
	case <-l.done:
		log.Println(string(p))
		return 0, fmt.Errorf("the log file '%s' is closed", l.filename)
	}
}

func (l *FileLogger) Close() error {
	close(l.done)
	return l.file.Close()
}

func (l *FileLogger) start() {
	l.wg.Add(1)

	go func() {
		defer l.wg.Done()

		for {
			select {
			case data := <-l.channel:
				l.writeToFile(data)
			case <-l.done:
				return
			}
		}
	}()
}

func (l *FileLogger) writeToFile(data []byte) {
	if l.file == nil {
		return
	}
	l.file.Write(data)
}

func (l *FileLogger) createLogFile() error {
	if _, err := os.Stat(l.filename); err != nil {
		basePath := path.Dir(l.filename)
		if _, err = os.Stat(basePath); err != nil {
			if err = os.MkdirAll(basePath, 0o755); err != nil {
				return err
			}
		}

		if l.file, err = os.Create(l.filename); err != nil {
			return err
		}
	} else if l.file, err = os.OpenFile(l.filename, os.O_APPEND|os.O_WRONLY, 0o600); err != nil {
		return err
	}

	if l.file != nil {
		syscall.CloseOnExec(int(l.file.Fd()))
	}
	return nil
}
