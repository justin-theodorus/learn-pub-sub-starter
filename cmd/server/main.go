package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connectionString := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Connection to RabbitMQ was successful")

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.SimpleQueueDurable,
		handlerLog(),
	)
	if err != nil {
		log.Fatalf("could not subscribe to game logs: %v", err)
	}

	gamelogic.PrintServerHelp()

replLoop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("Sending pause message...")
			publishPlayingState(publishCh, true)
		case "resume":
			fmt.Println("Sending resume message...")
			publishPlayingState(publishCh, false)
		case "quit":
			fmt.Println("Exiting...")
			break replLoop
		default:
			fmt.Println("Unknown command")
		}
	}

	fmt.Println("Shutting down Peril server...")
}

func publishPlayingState(ch *amqp.Channel, isPaused bool) {
	err := pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: isPaused,
	})
	if err != nil {
		log.Printf("could not publish playing state: %v", err)
	}
}

func handlerLog() func(routing.GameLog) pubsub.AckType {
	return func(gameLog routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		if err := gamelogic.WriteLog(gameLog); err != nil {
			fmt.Printf("could not write game log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
