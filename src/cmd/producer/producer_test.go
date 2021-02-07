package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/andreasanta/kafka_stream/src/model"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

var message = &model.Trade{
	ID:           "T1000",
	Version:      2,
	CounterParty: "CP-34",
	BookId:       "B10",
	Expired:      true,
	MaturesAt:    time.Date(2021, 01, 01, 12, 59, 0, 0, time.Local),
	CreatedAt:    time.Date(2021, 01, 01, 12, 59, 0, 0, time.Local),
}

type testReporterMock struct {
	errors []string
}

func newTestReporterMock() *testReporterMock {
	return &testReporterMock{errors: make([]string, 0)}
}

func (trm *testReporterMock) Errorf(format string, args ...interface{}) {
	trm.errors = append(trm.errors, fmt.Sprintf(format, args...))
}

func verifyDeserializedTrade(value []byte) error {

	var t model.Trade
	err := json.Unmarshal(value, &t)
	if err != nil {
		return err
	}

	if t.ID != "T1000" {
		return errors.New("Trade ID mismatch")
	}

	if t.Version != 2 {
		return errors.New("Trade version mismatch")
	}

	if t.CounterParty != "CP-34" {
		return errors.New("Trade counterparty mismatch")
	}

	if t.BookId != "B10" {
		return errors.New("Trade book id mismatch")
	}

	if t.Expired != true {
		return errors.New("Trade expired mismatch")
	}

	refDate := time.Date(2021, 01, 01, 12, 59, 0, 0, time.Local)
	if !t.MaturesAt.Equal(refDate) || !t.CreatedAt.Equal(refDate) {
		return errors.New("Trade dates mismatch")
	}

	return nil
}

func TestSerializationDeserialization(t *testing.T) {

	trm := newTestReporterMock()
	producerMock := mocks.NewAsyncProducer(trm, nil)
	producerMock.ExpectInputWithCheckerFunctionAndSucceed(verifyDeserializedTrade)

	// This produces one message and expect to match it on the "consumer" side
	producerMock.Input() <- &sarama.ProducerMessage{
		Topic: "trades",
		Value: message,
	}

	if len(trm.errors) > 0 {
		for _, e := range trm.errors {
			fmt.Println(e)
			t.Error(e)
		}
	}
}
