FROM golang:1.15.4-alpine3.12

RUN mkdir /app

ADD . /app

WORKDIR /app

ENV KAFKA_PEERS="kafka:9092"

RUN go build -o ./build/producer ./src/cmd/producer/producer.go

CMD ["/app/build/producer"]