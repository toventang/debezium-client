package client

type KafkaOptions struct {
	Addresses []string
	GroupID   string
	Topics    []string
}

type KafkaOption func(*KafkaOptions)

func newKafkaOption(addresses []string, groupID string, topics []string) KafkaOption {
	return func(opts *KafkaOptions) {
		opts.Addresses = addresses
		opts.GroupID = groupID
		opts.Topics = topics
	}
}
