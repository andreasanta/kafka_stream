package handler

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/andreasanta/kafka_stream/src/model"
	"gorm.io/gorm"
)

type DbConsumerGroupHandler struct {
	Db *gorm.DB
}

func (DbConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (DbConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h DbConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {

		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)

		// Here we write the message to the datbase in
		// a transactional fashion, we roll back if
		// necessary and mark the message as processed

		var t model.Trade
		err := json.Unmarshal(msg.Value, &t)
		if err != nil {
			sess.MarkMessage(msg, "Invalid Format")
			continue
		}

		h.Db.Transaction(func(tx *gorm.DB) error {

			result := h.Db.Create(&t)
			if result.Error != nil {
				fmt.Println(err)
				sess.MarkMessage(msg, fmt.Sprintf("%s", err))
				return err
			}

			// return nil will commit the whole transaction
			sess.MarkMessage(msg, "OK")
			return nil
		})

	}

	return nil
}
