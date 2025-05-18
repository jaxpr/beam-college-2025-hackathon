from datetime import timedelta

from temporalio import workflow

from agent_activities import data_architect_activity, data_engineer_activity, fetch_data_source_metadata_activity


@workflow.defn
class AnalyticsWorkflow:
    def __init__(self):
        self._state = {}

    @workflow.run
    async def run(self, user_query: str):
        # Add the inputs to the state, so we can leverage unified data fetching in the agents
        self._state['user_query'] = user_query

        # Fetch the metadata for the available data sources
        data_source_metadata = await workflow.execute_activity(
            fetch_data_source_metadata_activity,
            args=[],
            start_to_close_timeout=timedelta(minutes=5)
        )

        self._state["data_source_metadata"] = data_source_metadata

        # Generate data processing pipeline requirements
        data_analysis, requirements = await workflow.execute_activity(
            data_architect_activity,
            args=[self._state],
            start_to_close_timeout=timedelta(minutes=5)
        )

        self._state['data_analysis'] = data_analysis
        self._state['requirements'] = requirements

        # Generate data processing pipeline implementation
        pipeline_code, pipeline_documentation = await workflow.execute_activity(
            data_engineer_activity,
            args=[self._state],
            start_to_close_timeout=timedelta(minutes=5)
        )

        self._state['pipeline_code'] = pipeline_code
        self._state['pipeline_documentation'] = pipeline_documentation


        return self._state
