import json
import logging
import os
import time

import pyjq
from abc import ABC, abstractmethod
from typing import Dict
from confluent_kafka import TopicPartition
from kafka.entites import KafkaMessage, KafkaQuery
from repository.redis_repository import RedisRepository, RedisConfig
from confluent_kafka import Message
logger = logging.getLogger(__name__)


class ConsumerBase(ABC):
    def __init__(self, auth_config: Dict, query: KafkaQuery, partition_id: int, redis_config: Dict):
        self._conf = auth_config
        self._query = query
        self._repository = RedisRepository(redis_config)
        self._partition_id = partition_id
        self._consumer = self._create_consumer()

    @abstractmethod
    def _create_consumer(self) -> None:
        pass

    @abstractmethod
    def consume_messages(self) -> None:
        pass

    def assign_partition(self) -> None:
        logger.info(f"Topic {self._query.topic}, Assigning partition {self._partition_id} to consumer")
        self._consumer.assign([TopicPartition(self._query.topic, self._partition_id)])

    def close_consumer(self) -> None:
        logger.info("Closing consumer")
        self._consumer.close()

    def _watermark_offsets(self, msg: Message) -> bool:

        default_timeout = 5

        duration = (time.time() * 1000) - self._query.start_time

        logger.info(f"Topic {self._query.topic}, Getting watermark offsets for partition {self._partition_id}")
        low, high = self._consumer.get_watermark_offsets(TopicPartition(self._query.topic, msg.partition()))
        if low == -1 or high == -1:
            logger.error(f"Failed to get watermark offsets for partition {self._partition_id}")

        if msg.offset() >= high and (duration/6000) > default_timeout:
            logger.info(f"Reached end of partition {self._partition_id}")
            return True

    def _search_timeout(self) -> bool:

        duration = (time.time() * 1000) - self._query.start_time

        # duration is in milliseconds parse to minutes

        duration_minutes = duration / 60000

        if duration_minutes >= float(self._query.time_out):
            return True

        return False

    def _query_messages(self, message: KafkaMessage) -> None:

        if message.query.jq_query:
            self._apply_query(message)
            return

        if message.key:
            self._apply_key(message)

    def _apply_query(self, message: KafkaMessage) -> None:

        query = message.query.jq_query
        value = message.value
        result = {"result": pyjq.all(query, value)}
        self._founded(message, result)

    def _apply_key(self, message: KafkaMessage) -> None:

        if message.key == message.query.key:
            self._founded(message, message.value)

    def _founded(self, message: KafkaMessage, value: Dict) -> None:

        logger.info(f"Message found for query {message.query.id}")

        result = {"key": message.key, "value": value}

        try:
            # Adding Kafka metadata
            result["__kafka_offset"] = message.offset
            result["__kafka_partition"] = message.partition_id
            result["__kafka_publish_date_utc"] = int(message.timestamp / 1e6)  # Assuming timestamp is in microseconds

            store_value = json.dumps(result)
            self._repository.save(message.query.id, store_value)
        except json.JSONDecodeError as err:
            self._repository.search_exception(message.query.id, err)

        except Exception as err:
            self._repository.search_exception(message.query.id, err)
