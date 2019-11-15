package main

import (
	"context"
	"flag"
	"strings"
	"time"

	"github.com/toventang/debezium-client/adapter"
	"github.com/toventang/debezium-client/client"
	"github.com/toventang/debezium-client/subscriber"
)

func main() {
	var (
		kafkaAddress, groupID, topics                              string
		dstType, dstAddress, dstDatabase, dstUsername, dstPassword string
		timeout                                                    int
		fieldMapping                                               string
	)

	flag.StringVar(&kafkaAddress, "KAFKA_ADDRESS", "", "kafka addresses")
	flag.StringVar(&groupID, "KAFKA_GROUPID", "", "group id")
	flag.StringVar(&topics, "KAFKA_TOPICS", "", "topics")

	flag.StringVar(&dstType, "DST_TYPE", "", "destination database type")
	flag.StringVar(&dstAddress, "DST_ADDRESS", "", "destination database addresses")
	flag.StringVar(&dstDatabase, "DST_DATABASE", "", "database name")
	flag.IntVar(&timeout, "DST_TIMEOUT", 5, "R/W timeout")
	flag.StringVar(&dstUsername, "DST_USER", "", "user auth")
	flag.StringVar(&dstPassword, "DST_PASSWORD", "", "user auth")

	flag.StringVar(&fieldMapping, "FIELD_MAPPING", "", "fields mapping")
	flag.Parse()

	var tables []string
	t := strings.Split(topics, ",")
	for _, tn := range t {
		s := strings.SplitAfterN(tn, ".", 2)
		tables = append(tables, s[1])
	}

	ctx := context.Background()
	opts := client.Options{
		SubscriberOptions: subscriber.Options{
			Addresses: strings.Split(kafkaAddress, ","),
			GroupID:   groupID,
			Topics:    strings.Split(topics, ","),
		},
		AdapterOptions: adapter.Options{
			ConnectorType: adapter.ParseConnectorType(dstType),
			Addresses:     strings.Split(dstAddress, ","),
			Database:      dstDatabase,
			Timeout:       time.Duration(timeout) * time.Second,
			Tables:        tables,
			Username:      dstUsername,
			Password:      dstPassword,
			FieldMapping:  fieldMapping,
		},
	}
	cli, err := client.NewClient(opts)
	if err != nil {
		panic(err)
	}

	if err := cli.Start(ctx); err != nil {
		panic(err)
	}
	defer cli.Close()
}
