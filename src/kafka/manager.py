from multiprocessing import Pool
from typing import Type, Dict

from src.kafka.consumer_base import ConsumerBase
from src.kafka.entites import KafkaQuery


# Assuming kafka.consumer_base.ConsumerBase and kafka.entites.KafkaQuery are correctly imported

def _consumer_task(consumer_class, auth_config, query, partition_id, redis_config):
    consumer = consumer_class(
        auth_config=auth_config,
        query=query,
        partition_id=partition_id,
        redis_config=redis_config,
    )
    consumer.assign_partition()
    consumer.consume_messages()


class KafkaManager:
    def __init__(self):
        self.pool = Pool()  # Defaults to os.cpu_count() processes
        self.async_results = []

    def create_consumer(self,
                        consumer_class: Type[ConsumerBase],
                        query: KafkaQuery,
                        partition_ids: list,
                        redis_config: Dict,
                        auth_config: Dict
        ):

        for partition_id in partition_ids:
            task_args = (consumer_class, auth_config, query, partition_id, redis_config)
            async_result = self.pool.apply_async(_consumer_task, args=task_args)
            self.async_results.append(async_result)

    def stop_consumers(self) -> None:
        # Gracefully close the pool and wait for tasks to complete
        self.pool.close()
        self.pool.join()
        # Optionally, handle errors from workers
        for async_result in self.async_results:
            try:
                async_result.get()  # Retrieve result or re-raise exception
            except Exception as e:
                print(f"Error in consumer task: {e}")

# Ensure to adjust imports based on your actual package structure
