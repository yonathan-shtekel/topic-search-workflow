import logging
import os
from typing import Any, Dict
from confluent_kafka import Consumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
import io
import struct
from avro.io import BinaryDecoder, DatumReader
from kafka.consumer_base import ConsumerBase
from kafka.entites import KafkaMessage, KafkaQuery
from repository.redis_repository import RedisConfig


logger = logging.getLogger(__name__)

MAGIC_BYTES = 0


class ConsumerAvro(ConsumerBase):
    def __init__(self, auth_config: dict, query: KafkaQuery, partition_id: int, redis_config: Dict):
        super().__init__(auth_config, query, partition_id, redis_config)

    def _create_consumer(self):
        return Consumer(self._conf)

    def consume_messages(self):

        self._consumer.subscribe([self._query.topic])

        logger.info(f"Subscribed to topic {self._query.topic} for partition {self._partition_id}")

        while True:

            if self._search_timeout():
                logger.info("Search timeout reached")
                break

            try:
                msg = self._consumer.poll(10)
            except SerializerError as e:
                print("Message deserialization failed for {}: {}".format(msg, e))
                raise SerializerError

            if msg is None:
                continue

            if msg.error():
                logger.error(f"AvroConsumer error: {msg.error()}")
                return

            msg_value = msg.value()
            msg_key = msg.key()

            key, value = self.unpack(msg_key), self.unpack(msg_value)

            partition_id = msg.partition()
            offset = msg.offset()
            timestamp = msg.timestamp()

            kafka_message = KafkaMessage(
                key=key,
                value=value,
                partition_id=partition_id,
                offset=offset,
                timestamp=timestamp[1] if timestamp else None,
                query=self._query
            )

            self._query_messages(kafka_message)

            logger.info(f"Consumed message from topic {self._query.topic} for partition {self._partition_id}, offset {offset}")

            self._consumer.commit(msg, asynchronous=True)



    @staticmethod
    def unpack(payload: Any):
        magic, schema_id = struct.unpack('>bi', payload[:5])

        # Get Schema registry
        # Avro value format
        if magic == MAGIC_BYTES:
            register_client = CachedSchemaRegistryClient(url=os.environ['SCHEMA_REGISTRY_URL'])
            schema = register_client.get_by_id(schema_id)
            reader = DatumReader(schema)
            output = BinaryDecoder(io.BytesIO(payload[5:]))
            abc = reader.read(output)
            return abc
        # String key
        else:
            # If KSQL payload, exclude timestamp which is inside the key.
            # payload[:-8].decode()
            return payload.decode()
