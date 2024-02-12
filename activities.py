import logging
import random
from typing import List
from temporalio import activity
from kafka.admin import KafkaAdmin
from config import settings
from kafka.entites import KafkaQuery
from kafka.manager import KafkaManager
from kafka.consumer_avro import ConsumerAvro
from repository.redis_repository import RedisRepository

TASK_QUEUE_NAME = "topic_search"

logger = logging.getLogger(__name__)


@activity.defn
async def get_partitions(kafka_query: KafkaQuery) -> List[int]:
    logger.info(f"Getting partitions for topic {kafka_query.topic}")

    admin = KafkaAdmin(
        bootstrap_servers=settings.bootstrap_servers,
        sasl_username=settings.sasl_username,
        sasl_password=settings.sasl_password
    )

    admin.start()

    ids = admin.get_partitions_ids_by_topic(kafka_query.topic)

    logger.info(f"Partitions for topic {kafka_query.topic}: {ids}")

    return ids


@activity.defn
async def topic_search(kafka_query: KafkaQuery) -> List[str]:

    group_id = f"ks-group-{str(random.randint(1, 10000))}"
    redis_config = {"host": settings.redis_host, "port": settings.redis_port}
    redis_client: RedisRepository = RedisRepository(redis_config)

    logger.info(f"Starting consumer for topic {kafka_query.topic} with group id {group_id}")

    auth_config = {
        'bootstrap.servers': settings.bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': "earliest",
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': settings.sasl_username,
        'sasl.password': settings.sasl_password,
    }

    manager = KafkaManager()

    manager.create_consumer(
        consumer_class=ConsumerAvro,
        query=kafka_query,
        partition_ids=kafka_query.partition_ids,
        redis_config=redis_config,
        auth_config=auth_config
    )

    # Keep the manager running or handle this in your application logic

    manager.stop_consumers()

    logger.info(f"Consumer for topic {kafka_query.topic} with group id {group_id} stopped")

    results = redis_client.get_list(kafka_query.id)

    logger.info(f"Results for topic {kafka_query.topic}: {results}")

    return results
