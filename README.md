# cdc-streaming-pipeline
A cdc streaming pipeline :)

### Enter Spark and Kafka Container
docker exec -it cdc-kafka bash
docker exec -it cdc-spark-master bash

## Debezium
### Register Connector
./debezium/register_connector.sh
### Restart Connector
curl -X POST http://localhost:8083/connectors/neon-postgres-cdc/restart
### Delete Connector
curl -X DELETE http://localhost:8083/connectors/neon-postgres-cdc
### Connector Health
curl -s http://localhost:8083/connectors/neon-postgres-cdc/status | jq .

## Seeding
### One time
python simulator/seed_database.py --seed 5
### Continuous
python simulator/seed_database.py --continuous --interval 1.0

## Kafka
### Delete topics
/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic "neon_cdc.*"

### List topics
docker exec -it cdc-kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

### Consume messages
/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic neon_cdc.public.customer_transactions --from-beginning


## Spark
### Execute spark
/opt/spark/bin/spark-submit /opt/spark-apps/streaming/runner.py