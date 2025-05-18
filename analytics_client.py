import asyncio
import uuid
import logging
from temporalio.client import Client
from analytics_workflow import AnalyticsWorkflow

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    # Connect to Temporal server
    client = await Client.connect("localhost:7233")
    logger.info("Connected to Temporal server")

    # Generate a unique workflow ID
    workflow_id = f"analytics-workflow-{uuid.uuid4()}"

    # User query to process
    user_query = "Can you give me the top 3 selling knives with a magnolia or rosewood handle?"

    logger.info(f"Starting workflow with ID: {workflow_id}")
    logger.info(f"User query: {user_query}")

    # Start a workflow execution
    handle = await client.start_workflow(
        AnalyticsWorkflow.run,
        args=[user_query],
        id=workflow_id,
        task_queue="analytics-workflow-task-queue",
    )

    logger.info("Workflow started, waiting for result...")

    # Wait for the result
    result = await handle.result()

    logger.info("Workflow completed!")
    logger.info("Final state:")
    for key, value in result.items():
        if isinstance(value, str) and len(value) > 100:
            # Truncate long values for display
            logger.info(f"{key}: {value[:100]}...")
        else:
            logger.info(f"{key}: {value}")


if __name__ == "__main__":
    asyncio.run(main())