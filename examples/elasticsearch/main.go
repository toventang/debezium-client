package main

import (
	"context"
	"flag"
	"strings"

	client "github.com/toventang/debezium-client"
	"github.com/toventang/debezium-client/adapter"
)

func main() {
	var kafkaAddress, groupID, topics string
	kSet := flag.NewFlagSet("kafka", flag.ContinueOnError)
	kSet.StringVar(&kafkaAddress, "address", "192.168.50.199:9092", "kafka addresses")
	kSet.StringVar(&groupID, "group", "cdc.catalogs", "group id")
	kSet.StringVar(&topics, "topics", "dbserver1.inventory.customers", "topics")

	var esAddress, username, password string
	esSet := flag.NewFlagSet("elastic", flag.ContinueOnError)
	esSet.StringVar(&esAddress, "address", "http://192.168.50.138:9200", "elasticsearch addresses")
	esSet.StringVar(&username, "user", "", "user auth")
	esSet.StringVar(&password, "pwd", "", "password")

	ctx := context.Background()
	client, err := client.NewClient(client.KafkaOptions{
		Addresses: strings.Split(kafkaAddress, ","),
		GroupID:   groupID,
		Topics:    strings.Split(topics, ","),
	}, adapter.Options{
		ConnectorType: adapter.ELASTIC,
		Addresses:     strings.Split(esAddress, ","),
		Username:      username,
		Password:      password,
	})
	if err != nil {
		panic(err)
	}

	if err := client.Start(ctx); err != nil {
		panic(err)
	}
	defer client.Close()
}
