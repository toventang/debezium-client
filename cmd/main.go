package main

import (
	"context"
	"flag"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/toventang/debezium-client/internal/config"
	"github.com/toventang/debezium-client/internal/consumer"
	"github.com/toventang/debezium-client/pkg/log"
)

var configFile = flag.String("c", "etc/config.yaml", "set the config file, defaults to ~/etc/config.yaml")

func main() {
	flag.Parse()

	var (
		fileWriter io.WriteCloser
		c          = &config.Config{}
	)
	err := config.LoadFromFile(*configFile, c)
	if err != nil {
		fatal(err)
	}

	if strings.EqualFold(c.LogConf.Type, "file") {
		fileWriter, err = log.NewFileLogger(c.LogConf.Path)
		if err != nil {
			fatal(err)
		}
	}

	multiWriter := zerolog.MultiLevelWriter(os.Stdout, fileWriter)
	logger := zerolog.New(multiWriter).With().Caller().Logger()

	ctx := context.Background()
	consumer := consumer.NewConsumer(ctx, logger, c)

	logger.Info().Msg("Starting debezium client")
	consumer.Start()

	// blocking and graceful stop
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch
	consumer.Stop()
	logger.Info().Msg("Stopped debezium client")
}

func fatal(err error) {
	zlog.Fatal().Msg(err.Error())
	os.Exit(1)
}
