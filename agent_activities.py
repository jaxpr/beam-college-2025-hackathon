from temporalio import activity

@activity.defn
async def fetch_data_source_metadata_activity() -> str:
    import os

    from agents.tools.bigquery_tool import fetch_bigquery_metadata

    """
    Fetches metadata for all relevant data sources.
    Currently, it only fetches BigQuery metadata. This method can be expanded
    to include other data sources.

    Returns:
        str: A string containing the combined metadata from all configured data sources.
    """
    project_id = os.environ.get("PROJECT_ID")

    data_source_metadata = fetch_bigquery_metadata(project_id)
    return data_source_metadata


@activity.defn
async def data_architect_activity(state: dict) -> tuple[str, str]:
    import os
    from google import genai
    from agents.agent_implementations.data_architect import DataArchitectAgent

    project_id = os.environ["PROJECT_ID"]
    genai_location = os.environ["GENAI_LOCATION"]

    client = genai.Client(
        vertexai=True,
        project=project_id,
        location=genai_location
    )

    data_architect = DataArchitectAgent(state, client)

    data_analysis, requirements = data_architect.generate()

    return data_analysis, requirements


@activity.defn
async def data_engineer_activity(state: dict) -> tuple[str, str]:
    import os
    from google import genai
    from agents.agent_implementations.data_engineer import DataEngineerAgent

    project_id = os.environ["PROJECT_ID"]
    genai_location = os.environ["GENAI_LOCATION"]
    output_bucket = os.environ["OUTPUT_BUCKET"]

    client = genai.Client(
        vertexai=True,
        project=project_id,
        location=genai_location
    )

    state["output_bucket"] = output_bucket

    data_engineer = DataEngineerAgent(state, client)

    pipeline_code = data_engineer.generate()

    return pipeline_code

@activity.defn
async def run_beam_pipeline_activity(state: dict):
    pass