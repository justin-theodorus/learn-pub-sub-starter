package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not get username: %v", err)
	}

	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+username,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause messages: %v", err)
	}

	armyMovesKey := routing.ArmyMovesPrefix + "." + username
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		armyMovesKey,
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gameState, publishCh),
	)
	if err != nil {
		log.Fatalf("could not subscribe to army moves: %v", err)
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueDurable,
		handlerWar(gameState, publishCh),
	)
	if err != nil {
		log.Fatalf("could not subscribe to war declarations: %v", err)
	}

replLoop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			if err := gameState.CommandSpawn(words); err != nil {
				fmt.Println(err)
			}
		case "move":
			move, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, armyMovesKey, move)
			if err != nil {
				fmt.Printf("could not publish move: %v\n", err)
				continue
			}
			fmt.Println("Move published successfully")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if err := spamGameLogs(publishCh, username, words); err != nil {
				fmt.Println(err)
			}
		case "quit":
			gamelogic.PrintQuit()
			break replLoop
		default:
			fmt.Println("Unknown command")
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		switch gs.HandleMove(move) {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("could not publish war declaration: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)

		var message string
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon:
			message = fmt.Sprintf("%s won a war against %s", winner, loser)
		case gamelogic.WarOutcomeDraw:
			message = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
		default:
			fmt.Printf("error: unknown war outcome %v\n", outcome)
			return pubsub.NackDiscard
		}

		if err := publishGameLog(publishCh, rw.Attacker.Username, message); err != nil {
			fmt.Printf("could not publish game log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}

func publishGameLog(publishCh *amqp.Channel, username, message string) error {
	return pubsub.PublishGob(
		publishCh,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+username,
		routing.GameLog{
			CurrentTime: time.Now(),
			Message:     message,
			Username:    username,
		},
	)
}

func spamGameLogs(publishCh *amqp.Channel, username string, words []string) error {
	if len(words) < 2 {
		return errors.New("usage: spam <n>")
	}
	count, err := strconv.Atoi(words[1])
	if err != nil || count < 1 {
		return fmt.Errorf("error: %s is not a valid number of logs", words[1])
	}

	for i := 0; i < count; i++ {
		if err := publishGameLog(publishCh, username, gamelogic.GetMaliciousLog()); err != nil {
			return fmt.Errorf("could not publish spam log %d of %d: %w", i+1, count, err)
		}
	}
	fmt.Printf("Published %d malicious logs\n", count)
	return nil
}
