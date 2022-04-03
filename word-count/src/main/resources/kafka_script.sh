#!/bin/bash

/opt/kafka_2.13-2.8.1/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic word-count-input

/opt/kafka_2.13-2.8.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic word-count-output --from-beginning \
--formatter kafka.tools.DefaultMessageFormatter \
--property print.key=true \
--property print.value=true \
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
