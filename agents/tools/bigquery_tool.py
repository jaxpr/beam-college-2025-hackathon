from google.cloud import bigquery
import datetime


def fetch_bigquery_metadata(project_id: str) -> str:
    """
    Lists all tables and their column metadata from all datasets in a GCP project
    and returns a formatted markdown report.

    Args:
        project_id (str): The GCP project ID.

    Returns:
        str: A markdown formatted report containing table and column metadata.
    """
    # Initialize the BigQuery client
    client = bigquery.Client(project=project_id)

    # Initialize markdown content
    markdown = f"# BigQuery Metadata Report\n\n"
    markdown += f"## Project: `{project_id}`\n"
    markdown += f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

    # Get all datasets in the project
    datasets = list(client.list_datasets())

    if not datasets:
        markdown += f"No datasets found in project `{project_id}`\n"
        return markdown

    markdown += f"Found {len(datasets)} datasets in project `{project_id}`\n\n"

    # Iterate through each dataset
    for dataset in datasets:
        dataset_id = dataset.dataset_id
        markdown += f"## Dataset: `{dataset_id}`\n\n"

        # Get all tables in the dataset
        tables = list(client.list_tables(dataset_id))

        if not tables:
            markdown += f"No tables found in dataset `{dataset_id}`\n\n"
            continue

        markdown += f"Found {len(tables)} tables in dataset `{dataset_id}`\n\n"

        # Iterate through each table
        for table in tables:
            table_id = f"{project_id}.{dataset_id}.{table.table_id}"
            markdown += f"### Table: `{table.table_id}`\n\n"

            try:
                # Get the table details including schema
                table_details = client.get_table(table_id)

                # Table metadata
                markdown += "#### Table Metadata\n\n"
                markdown += "| Property | Value |\n"
                markdown += "| --- | --- |\n"
                markdown += f"| Full Table ID | `{table_id}` |\n"
                markdown += f"| Description | {table_details.description or 'N/A'} |\n"
                markdown += f"| Created | {table_details.created.strftime('%Y-%m-%d %H:%M:%S')} |\n"
                markdown += f"| Last Modified | {table_details.modified.strftime('%Y-%m-%d %H:%M:%S')} |\n"
                markdown += f"| Number of Rows | {table_details.num_rows or 'N/A'} |\n"
                markdown += f"| Size in Bytes | {table_details.num_bytes or 'N/A'} |\n"
                markdown += f"| Table Type | {table_details.table_type} |\n\n"

                # Column metadata
                if table_details.schema:
                    markdown += "#### Column Metadata\n\n"
                    markdown += "| Column Name | Data Type | Mode | Description |\n"
                    markdown += "| --- | --- | --- | --- |\n"

                    for field in table_details.schema:
                        markdown += f"| {field.name} | {field.field_type} | {field.mode} | {field.description or 'N/A'} |\n"
                else:
                    markdown += "This table has no schema defined.\n"

                markdown += "\n---\n\n"

            except Exception as e:
                markdown += f"Error processing table {table_id}: {str(e)}\n\n---\n\n"

    return markdown
