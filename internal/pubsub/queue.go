package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

const deadLetterExchange = "peril_dlx"

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %w", err)
	}

	isTransient := queueType == SimpleQueueTransient
	queue, err := ch.QueueDeclare(
		queueName,
		queueType == SimpleQueueDurable,
		isTransient,
		isTransient,
		false,
		amqp.Table{"x-dead-letter-exchange": deadLetterExchange},
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue %s: %w", queueName, err)
	}

	if err := ch.QueueBind(queue.Name, key, exchange, false, nil); err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue %s: %w", queueName, err)
	}

	return ch, queue, nil
}
