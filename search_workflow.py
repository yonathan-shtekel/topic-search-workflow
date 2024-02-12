from datetime import timedelta
from temporalio import workflow

from kafka.entites import KafkaQuery

with workflow.unsafe.imports_passed_through():
    from activities import get_partitions, topic_search


@workflow.defn
class KafkaSearchWorkflow:
    @workflow.run
    async def run(self, kafka_query: KafkaQuery) -> None:
        ids = await workflow.execute_activity(
            get_partitions,
            kafka_query,
            start_to_close_timeout=timedelta(seconds=300),
        )

        kafka_query.partition_ids = ids

        return await workflow.execute_activity(
            topic_search,
            kafka_query,
            start_to_close_timeout=timedelta(minutes=5),
        )
