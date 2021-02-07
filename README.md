

## TODO

- Better retry policy/logging when sending fails
- Use Kafka Streams to perform message validation and foward them to a "clean" topic
- Use Apache Avro to define the schema instead of pure JSON, for future maintainability
- I've assumed only Trade ID and Version are in Primary Key, but more PKs can be added by annotating the db struct
- For simplicity the number of topic partions have been limited to 1