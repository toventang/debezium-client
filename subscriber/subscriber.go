package subscriber

import (
	"context"

	"github.com/Shopify/sarama"
)

type Subscriber struct {
	kafka   sarama.ConsumerGroup
	handler handler
	opts    Options
}

func NewSubscriber(opt ...Option) (Subscriber, error) {
	opts := NewOptions(opt...)
	subscriber := Subscriber{opts: opts, handler: handler{opts.connector}}

	conf := sarama.NewConfig()
	conf.Version = sarama.V0_11_0_0
	cg, err := sarama.NewConsumerGroup(opts.Addresses, opts.GroupID, conf)
	if err != nil {
		return subscriber, err
	}

	subscriber.kafka = cg
	return subscriber, nil
}

func (sub Subscriber) Subscribe(ctx context.Context) error {
	return sub.kafka.Consume(ctx, sub.opts.Topics, &sub)
}

func (sub Subscriber) Close() error {
	if sub.kafka != nil {
		return sub.kafka.Close()
	}
	return nil
}

func (sub Subscriber) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := sub.handler.process(msg); err != nil {
			break
		} else {
			sess.MarkMessage(msg, "")
		}
	}
	return nil
}

func (Subscriber) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (Subscriber) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
