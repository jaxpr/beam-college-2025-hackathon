import asyncio
import concurrent.futures

from temporalio.client import Client
from temporalio.worker import Worker

from agent_activities import fetch_data_source_metadata_activity, data_architect_activity, data_engineer_activity
from analytics_workflow import AnalyticsWorkflow

TASK_QUEUE = "analytics-workflow-task-queue"
TEMPORAL_SERVER_HOST = "localhost:7233"
MAX_WORKERS = 4

async def main():
    temporal_client = await Client.connect(target_host=TEMPORAL_SERVER_HOST)

    # Run the worker
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as activity_executor:
        worker = Worker(
            temporal_client,
            task_queue=TASK_QUEUE,
            workflows=[AnalyticsWorkflow],
            activities=[
                fetch_data_source_metadata_activity,
                data_architect_activity,
                data_engineer_activity,
            ],
            activity_executor=activity_executor,
        )

        print(f"Starting worker, connecting to task queue: {TASK_QUEUE}")
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

