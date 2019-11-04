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
	var kafkaAddress, groupID, topics string
	kSet := flag.NewFlagSet("kafka", flag.ContinueOnError)
	kSet.StringVar(&kafkaAddress, "address", "192.168.50.199:9092", "kafka addresses")
	kSet.StringVar(&groupID, "group", "cdc.catalogs.subscriber", "group id")
	kSet.StringVar(&topics, "topics", "catalogdbs.public.catalogs,catalogdbs.public.templates", "topics")

	var esAddress, username, password string
	var timeout int
	var tables []string
	esSet := flag.NewFlagSet("elastic", flag.ContinueOnError)
	esSet.StringVar(&esAddress, "address", "http://192.168.50.138:9200", "elasticsearch addresses")
	esSet.IntVar(&timeout, "timeout", 5000, "")
	esSet.StringVar(&username, "user", "", "user auth")
	esSet.StringVar(&password, "pwd", "", "password")

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
			ConnectorType: adapter.ELASTIC,
			Addresses:     strings.Split(esAddress, ","),
			Timeout:       time.Duration(timeout) * time.Second,
			Tables:        tables,
			Username:      username,
			Password:      password,
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
