import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker
from activities import TASK_QUEUE_NAME, get_partitions, topic_search
from search_workflow import KafkaSearchWorkflow

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


async def main():
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE_NAME,
        workflows=[KafkaSearchWorkflow],
        activities=[get_partitions, topic_search],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
