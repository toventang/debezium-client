package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v3"
)

type (
	Config struct {
		LogConf    LogConf         `yaml:"LogConf"`
		Kafka      KafkaConf       `yaml:"Kafka"`
		Connectors []ConnectorConf `yaml:"Connectors"`
		Timeout    int64           `yaml:"Timeout"`
	}

	LogConf struct {
		Type string `yaml:"Type"`
		Path string `yaml:"Path"`
	}

	KafkaConf struct {
		Brokers  []string `yaml:"Brokers"`
		Group    string   `yaml:"Group"`
		Topics   []string `yaml:"Topics"`
		MinBytes int      `yaml:"MinBytes"`
		MaxBytes int      `yaml:"MaxBytes"`
	}

	ConnectorConf struct {
		Type       string      `yaml:"Type"`
		DataSource string      `yaml:"DataSource"`
		Tables     []TableConf `yaml:"Tables"`
	}

	TableConf struct {
		Name         string   `yaml:"Name"`
		PrimaryKey   string   `yaml:"PrimaryKey"`
		FieldMapping []string `yaml:"FieldMapping"`
	}
)

// load config from file into v
func LoadFromFile(file string, v interface{}) error {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, v)
}
