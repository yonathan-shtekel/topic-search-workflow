import asyncio
import logging

from temporalio.client import Client
from activities import TASK_QUEUE_NAME
from kafka.entites import KafkaQuery
from search_workflow import KafkaSearchWorkflow

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


async def main():
    topic = "core.fct.webhook-order.de-core-bb-e2e-tests-c90d163"
    jq_filter = '.billingAddresses[] | select(.country == "United States of America") | .city'

    kafka_query = KafkaQuery(
        topic=topic,
        jq_query=jq_filter,
    )

    workflow_id = f"topic-ssearch-workflow-{kafka_query.id}"

    client = await Client.connect("localhost:7233")

    await client.execute_workflow(
        KafkaSearchWorkflow.run,
        kafka_query,
        id=workflow_id,
        task_queue=TASK_QUEUE_NAME,
    )


if __name__ == "__main__":
    asyncio.run(main())
