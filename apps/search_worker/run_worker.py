import asyncio
import logging
from temporalio.client import Client
from temporalio.worker import Worker
from config import settings
from src.workflow.activities import TASK_QUEUE_NAME, get_partitions, topic_search
from src.workflow.search_workflow import KafkaSearchWorkflow

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


async def main():
    client = await Client.connect(settings.temporal_url)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE_NAME,
        workflows=[KafkaSearchWorkflow],
        activities=[get_partitions, topic_search],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
