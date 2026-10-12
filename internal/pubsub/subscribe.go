package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

const prefetchCount = 10

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, func(body []byte) (T, error) {
		var val T
		err := json.Unmarshal(body, &val)
		return val, err
	})
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, func(body []byte) (T, error) {
		var val T
		err := gob.NewDecoder(bytes.NewReader(body)).Decode(&val)
		return val, err
	})
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue %s: %w", queueName, err)
	}

	if err := ch.Qos(prefetchCount, 0, false); err != nil {
		return fmt.Errorf("could not set prefetch count for queue %s: %w", queueName, err)
	}

	deliveries, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume queue %s: %w", queueName, err)
	}

	go func() {
		for delivery := range deliveries {
			val, err := unmarshaller(delivery.Body)
			if err != nil {
				log.Printf("could not unmarshal message from %s: %v", queueName, err)
				// discard instead of requeueing, a malformed message would never succeed
				settle(delivery, NackDiscard)
				continue
			}

			settle(delivery, handler(val))
		}
	}()

	return nil
}

func settle(delivery amqp.Delivery, ackType AckType) {
	var err error
	switch ackType {
	case Ack:
		fmt.Println("Ack")
		err = delivery.Ack(false)
	case NackRequeue:
		fmt.Println("NackRequeue")
		err = delivery.Nack(false, true)
	default:
		fmt.Println("NackDiscard")
		err = delivery.Nack(false, false)
	}
	if err != nil {
		log.Printf("could not settle message: %v", err)
	}
}
