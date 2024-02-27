import logging
import os
from typing import Any, Dict
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
import io
import struct
from avro.io import BinaryDecoder, DatumReader
from src.kafka.consumer_base import ConsumerBase
from src.kafka.entites import KafkaMessage, KafkaQuery

logger = logging.getLogger(__name__)

MAGIC_BYTES = 0


class ConsumerAvro(ConsumerBase):
    def __init__(self, auth_config: dict, query: KafkaQuery, partition_id: int, redis_config: Dict):
        super().__init__(auth_config, query, partition_id, redis_config)

    def _create_consumer(self):
        return Consumer(self._conf)

    def _consume_impl(self) -> bool:

        try:
            msg = self._consumer.poll(0.1)
        except SerializerError as e:
            print("Message deserialization failed for {}: {}".format(msg, e))
            raise SerializerError

        if msg is None:
            return False

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                logging.info(f"Reached the end of partition {msg.partition()} at offset {msg.offset()}")
            else:
                logger.error(f"AvroConsumer error: {msg.error()}")

            return True

        msg_value = msg.value()
        msg_key = msg.key()
        partition_id = msg.partition()
        offset = msg.offset()
        timestamp = msg.timestamp()

        if self._is_message_duplicate(msg):
            logger.info(f"Message is duplicate, skipping")
            return False

        key, value = self.unpack(msg_key), self.unpack(msg_value)

        kafka_message = KafkaMessage(
            key=key,
            value=value,
            partition_id=partition_id,
            offset=offset,
            timestamp=timestamp[1] if timestamp else None,
            query=self._query
        )

        self._query_messages(kafka_message)

        logger.info(
            f"Consumed message from topic {self._query.topic} for partition {self._partition_id}, offset {offset}")

        self._consumer.commit(msg, asynchronous=False)

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
