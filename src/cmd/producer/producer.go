package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/andreasanta/kafka_stream/src/model"
	"github.com/brianvoe/gofakeit/v6"

	"flag"
	"log"
	"math/rand"
	"os"
)

var (
	brokers  = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	verbose  = flag.Bool("verbose", false, "Turn on Kafka client logging")
	interval = flag.Int("ms", 543, "Number of ms to wait between consecutive sends")
)

func main() {

	flag.Parse()

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[kafka] ", log.LstdFlags)
	}

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	producer := createAysncProducer(brokerList)

	fmt.Println("Press Ctrl+C to stop producer")

	for {

		trade := &model.Trade{
			ID:           fmt.Sprintf("T%d", rand.Intn(100000)),
			Version:      uint(rand.Intn(100)),
			CounterParty: fmt.Sprintf("CP-%d", rand.Intn(100)),
			BookId:       fmt.Sprintf("B%d", rand.Intn(100)),
			Expired:      rand.Intn(2) != 0,
			MaturesAt:    gofakeit.Date(),
			CreatedAt:    gofakeit.Date(),
		}

		log.Printf("Producing trade ID %s", trade.ID)

		producer.Input() <- &sarama.ProducerMessage{
			Topic: "trades",
			Value: trade, // This used the Encode() method of the struct and marshals into JSON
		}

		time.Sleep(time.Duration(*interval) * time.Millisecond)
	}
}

func createAysncProducer(brokerList []string) sarama.AsyncProducer {

	/**
	 * We select an async producer all partitions ack to ensure
	 * data is sent quickly but also never lost.
	 *
	 * !It's important trading data we cannot afford to miss.
	 */
	config := sarama.NewConfig()

	config.ClientID = "TradesProducer"
	config.Producer.RequiredAcks = sarama.WaitForAll         // Ensure all partitions are written to, not just the leader
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages -> higher throughput, less net I/O
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	var producer sarama.AsyncProducer
	var err error

	for {
		producer, err = sarama.NewAsyncProducer(brokerList, config)
		if err != nil {
			log.Println("Failed to start Sarama producer:", err)
			log.Println("Waiting and retrying:", err)
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	// Go routine that handles temporary writing issues
	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write access log entry:", err)
		}
	}()

	return producer
}
