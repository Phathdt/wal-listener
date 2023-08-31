package publisher

import (
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-jetstream/pkg/jetstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/goccy/go-json"
	"github.com/ihippik/wal-listener/v2/config"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

type natsPublisher struct {
	publisher *jetstream.Publisher
	logger    *logrus.Entry
}

func NewNatsPublisher(cfg *config.PublisherCfg, l *logrus.Entry) (*natsPublisher, error) {
	options := []nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.Timeout(30 * time.Second),
		nats.ReconnectWait(1 * time.Second),
	}
	marshaler := &jetstream.GobMarshaler{}
	logger := watermill.NewStdLogger(false, false)

	publisher, err := jetstream.NewPublisher(
		jetstream.PublisherConfig{
			URL:         cfg.Address,
			NatsOptions: options,
			Marshaler:   marshaler,
		},
		logger,
	)

	if err != nil {
		return nil, fmt.Errorf("nats connection: %w", err)
	}

	return &natsPublisher{publisher: publisher, logger: l}, nil
}

func (n *natsPublisher) Publish(topic string, event Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := message.NewMessage(watermill.NewUUID(), payload)
	if err = n.publisher.Publish(topic, msg); err != nil {
		return err
	}

	n.logger.Infof("natsPublisher %s message = %+v\n", topic, string(msg.Payload))

	return nil
}
