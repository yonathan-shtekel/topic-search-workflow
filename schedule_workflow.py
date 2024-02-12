# # @@@SNIPSTART data-pipeline-schedule-workflow-python
# import asyncio
# from datetime import timedelta
#
# from temporalio.client import (
#     Client,
#     Schedule,
#     ScheduleActionStartWorkflow,
#     ScheduleIntervalSpec,
#     ScheduleSpec,
#     ScheduleState,
# )
#
# from activities import TASK_QUEUE_NAME
# from search_workflow import KafkaSearchWorkflow
#
#
# async def main():
#     client = await Client.connect("localhost:7233")
#     await client.create_schedule(
#         "top-stories-every-10-hours",
#         Schedule(
#             action=ScheduleActionStartWorkflow(
#                 KafkaSearchWorkflow.run,
#                 id="temporal-community-workflow",
#                 task_queue=TASK_QUEUE_NAME,
#             ),
#             spec=ScheduleSpec(
#                 intervals=[ScheduleIntervalSpec(every=timedelta(hours=10))]
#             ),
#         ),
#     )
#
#
# if __name__ == "__main__":
#     asyncio.run(main())
# # @@@SNIPEND
