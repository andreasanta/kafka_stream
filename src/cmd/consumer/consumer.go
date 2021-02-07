package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/andreasanta/kafka_stream/src/cmd/consumer/handler"
	"github.com/andreasanta/kafka_stream/src/model"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	brokers = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The Kafka brokers to connect to, as a comma separated list")
	verbose = flag.Bool("verbose", true, "Turn on Kafka client logging")
	topic   = flag.String("topic", "trades", "Listen on specified topic")
	dbhost  = flag.String("dbhost", "pgsql", "Specified the DB host to connect to")
)

func dbInit() *gorm.DB {
	dsn := fmt.Sprintf("host=%s user=gorm password=gorm dbname=trades port=5432 sslmode=disable TimeZone=Europe/Madrid", *dbhost)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalln("Unable to connect to PGSQL database")
	}

	err = db.AutoMigrate(&model.Trade{})
	if err != nil {
		log.Fatalln("Unable to connect to migrate database")
	}

	return db
}

func main() {

	flag.Parse()

	if *brokers == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[kafka] ", log.LstdFlags)
	}

	var db *gorm.DB = dbInit()
	sqlDb, err := db.DB()
	if err != nil {
		log.Fatal(err)
	}
	defer sqlDb.Close()

	brokerList := strings.Split(*brokers, ",")
	log.Printf("Kafka brokers: %s", strings.Join(brokerList, ", "))

	group := createConsumerGroup(brokerList)
	defer group.Close()

	// Iterate over consumer sessions.
	ctx := context.Background()
	for {

		topics := []string{"trades"}
		handler := handler.DbConsumerGroupHandler{
			Db: db,
		}

		err := group.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}

}

func createConsumerGroup(brokerList []string) sarama.ConsumerGroup {

	config := sarama.NewConfig()
	config.ClientID = "TradesConsumer"

	group, err := sarama.NewConsumerGroup(brokerList, "trade-consumers", config)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	// Track errors
	go func() {
		for err := range group.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	return group
}
