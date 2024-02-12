from typing import Type, Dict
from kafka.consumer_base import ConsumerBase
from kafka.entites import KafkaQuery




def _consumer_task(args):
    consumer_class, auth_config, query, partition_id, repository = args
    consumer = consumer_class(
        auth_config=auth_config,
        query=query,
        partition_id=partition_id,
        redis_repository=repository,
    )
    consumer.assign_partition()
    consumer.consume_messages()


class KafkaManager:
    def __init__(self):
        # self.pool = mp.Pool(mp.cpu_count())
        self.consumers = []
        self.async_results = []

    def create_consumer(self,
                        consumer_class: Type[ConsumerBase],
                        query: KafkaQuery,
                        partition_ids: list,
                        redis_config: Dict,
                        auth_config: Dict
    ) -> None:
        # tasks = [(consumer_class, auth_config, query, partition_id, redis_config) for partition_id in partition_ids]
        # # Using apply_async instead of map for more complex setups
        # for task in tasks:
        #     async_result = self.pool.apply_async(_consumer_task, args=(task,))
        #     async_result.get()  # This is to catch any exceptions raised in the worker process



        for partition_id in partition_ids:
            consumer = consumer_class(
                auth_config=auth_config,
                query=query,
                partition_id=partition_id,
                redis_config=redis_config,
            )
            consumer.assign_partition()
            consumer.consume_messages()
            # self.consumers.append(consumer)

    def stop_consumers(self) -> None:
        pass
        # self.pool.close()
        # self.pool.join()