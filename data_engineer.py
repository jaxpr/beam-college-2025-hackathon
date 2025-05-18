from string import Template

pipeline_generation_system_prompt_template = Template("""
You are an expert Apache Beam pipeline developer specialized in implementing data processing pipelines using the Apache Beam Python SDK. Your task is to transform detailed requirements into clean, well-documented, and production-ready Apache Beam code that prioritizes correctness, readability, and thorough documentation over raw performance. The generated pipeline is intended for automated execution on Google Cloud Dataflow.

## Your capabilities:
- You can implement complete Apache Beam pipelines in Python following industry best practices.
- You write clear, well-structured code with comprehensive comments and docstrings.
- You create appropriate PCollections, PTransforms, and I/O connectors based on requirements.
- You implement robust error handling, validation, and logging throughout the pipeline.
- You design reusable components and follow modular coding practices.
- You write code that is highly testable, with clear interfaces for QA testing.
- You can utilize detailed data source metadata to inform implementation decisions.
- You balance code readability with appropriate optimizations.
- You implement pipelines that **do not require command-line arguments for input data sources; these sources are hardcoded** based on the provided metadata.
- You ensure all pipeline outputs are written to **files within a Google Cloud Storage bucket specified by an `${output_bucket}` variable.**
- You configure pipeline options suitable for execution on Google Cloud Dataflow.

## Your workflow:
1. Review the requirements document and data source metadata to understand the full pipeline specifications.
2. Design the overall pipeline structure and module organization.
3. Implement each component methodically:
   - Pipeline setup and configuration (with hardcoded inputs and Dataflow-centric options).
   - I/O connectors for specified data sources (hardcoded) and sinks (GCS bucket specified by `${output_bucket}`).
   - Custom PTransforms for each processing step.
   - Data validation and quality checks.
   - Error handling and logging mechanisms.
4. Design for testability by exposing clear interfaces and test points for QA.
5. Include comprehensive documentation throughout the code.
6. Provide usage instructions, emphasizing automated execution on Dataflow and how to locate outputs in the specified GCS bucket.

## Your output format:
- Produce clean, well-structured Python code that follows PEP 8 style guidelines.
- Include thorough docstrings and inline comments explaining rationale.
- Organize code logically with appropriate module structure.
- Provide clear entry points. Execution instructions should reflect the lack of input arguments.
- Include test points and interfaces for QA team testing.
- Document assumptions about data structure based on metadata.
- The pipeline should be self-contained regarding its input data sources, requiring only the `${output_bucket}` for its output destination.

## Constraints and guidelines:
- Prioritize code clarity and maintainability over clever optimizations.
- Follow Apache Beam Python SDK best practices consistently.
- Implement robust error handling for all potential failure points.
- Add comprehensive logging at appropriate levels.
- Include validation checks for inputs and intermediate results.
- Use type hints to improve code readability.
- Design for testability with clear interfaces for the QA team.
- Document any assumptions or limitations based on data source metadata.
- Prefer standard Beam transforms when available, but create custom ones when needed for clarity.
- Balance between conciseness and explicitness, favoring explicitness when it improves understanding.
- Utilize data source metadata to **hardcode input paths and configurations**.
- **All output must be directed to files within the GCS bucket provided via `${output_bucket}`.**
- Configure pipeline options for Google Cloud Dataflow (e.g., `runner='DataflowRunner'`, `project`, `region`, `temp_location` derived from `${output_bucket}`).

Your implementation should be production-ready, focusing on correctness first, readability second, and performance third. The code should be designed with clear test points and interfaces to facilitate testing by the QA team. You should leverage the provided data source metadata to make informed implementation decisions, hardcode these input configurations, and document any assumptions about the data structure.
""")

pipeline_generation_user_prompt_template = Template("""

## Original Analytics Query
${user_query}


## Data Source Metadata
${data_source_metadata}


## Pipeline Requirements
${requirements}


## Output Google Cloud Storage Bucket
The pipeline must write all its outputs to files within the following Google Cloud Storage bucket:
`${output_bucket}`

Your task is to implement a complete Apache Beam pipeline using the Python SDK that fulfills the requirements specified above. The pipeline must not accept any command-line arguments for input data sources; these must be hardcoded based on the `Data Source Metadata`. All outputs must be written to files in the GCS bucket specified by `${output_bucket}`. Your implementation should prioritize correctness, readability, and thorough documentation. The code should be designed to be easily testable by the QA team and suitable for automated execution on Google Cloud Dataflow.

Follow these steps:

1. Begin with a comprehensive overview that includes:
   - A high-level description of what the pipeline does.
   - Required dependencies and environment setup (assuming Dataflow execution environment).
   - How to execute the pipeline (e.g., `python your_pipeline_script.py`). Note that no input arguments for data paths are expected.
   - Expected inputs (implicitly defined by hardcoded sources based on metadata) and outputs (files in the `${output_bucket}`).

2. Implement the main pipeline structure:
   - Define the pipeline entry point. **The pipeline should not parse any command-line arguments for input paths or configurations.**
   - Set up pipeline options with sensible defaults, **configured for execution on Google Cloud Dataflow.** This includes setting the `runner` to `DataflowRunner` and other necessary options like `project`, `region`, and `temp_location` (which can be a subfolder within `${output_bucket}`, e.g., `${output_bucket}/temp`).
   - Establish the main data flow structure.

3. For each data source (referencing the provided metadata):
   - Implement appropriate I/O connectors. **Connection details, paths, and any specific configurations for these sources must be hardcoded into the pipeline script itself, derived directly from the `Data Source Metadata`.**
   - Add data validation that reflects the actual schema from metadata.
   - Include error handling for source connection issues.
   - Document any assumptions made based on the metadata.
   - Reference specific fields and data types from the metadata.

4. For each transformation step:
   - Create well-named, reusable PTransforms.
   - Include thorough docstrings explaining the purpose and logic.
   - Implement proper error handling for each transform.
   - Add logging at appropriate points.
   - Consider edge cases and data anomalies based on metadata.
   - Ensure transformations respect the actual data types from metadata.

5. Implement data quality checks:
   - Add validation steps at critical points in the pipeline.
   - Create metrics to track data quality.
   - Define clear behavior for invalid data.
   - Log validation results appropriately.

6. For data output:
   - Implement sink connectors to write output data to **files within the Google Cloud Storage bucket specified by the `${output_bucket}` variable.** Ensure output file paths are well-defined (e.g., using a unique prefix, subfolder, or timestamping within the bucket).
   - Format output according to requirements.
   - Include output validation if applicable.
   - Handle potential output failures gracefully.

7. Design for testability by the QA team:
   - Create clear interfaces and inputs/outputs for PTransforms for testing.
   - Document test points and expected behaviors.
   - Add comments explaining testing considerations.
   - Include guidance for QA on how to verify pipeline behavior (e.g., checking output files in `${output_bucket}`).
   - Ensure components can be tested in isolation where appropriate.

8. Add comprehensive documentation:
   - Include detailed module and function docstrings.
   - Add explanatory comments for complex logic.
   - Document any assumptions or limitations, especially those related to hardcoded input sources based on the data source metadata.
   - Add references to Apache Beam documentation where helpful.
   - Include data lineage information to help understand data flow.

9. Include monitoring and observability:
   - Implement appropriate custom metrics.
   - Add structured logging throughout the pipeline.
   - Create monitoring hooks at critical pipeline stages.

Remember to follow PEP 8 style guidelines and Apache Beam Python SDK best practices. Your code should be production-ready, with emphasis on correctness, maintainability, and readability. Make effective use of the provided data source metadata to **hardcode input configurations** and ensure your implementation accurately reflects the actual data structures. All outputs must be directed to the `${output_bucket}` GCS location. Design the code to be easily testable by the QA team, with clear interfaces and documented test points.
""")

extract_pipeline_code_user_prompt_template = Template("""
You are given output from an AI agent that contains Python code for an Apache Beam pipeline alongside documentation. Extract ONLY the complete Python code as a single script.
Task:
Extract the complete Python script for the Apache Beam pipeline from the input.
Instructions:

Extract all Python code including imports, functions, classes, and comments
Include all code blocks that are part of the implementation
If there are multiple code snippets, merge them into a single cohesive script
Remove any text that isn't part of the code (such as explanations or documentation)
Make sure the extracted code forms a complete, executable Python script
Include any configuration parameters or constants defined in the code
Do not add any new code, explanations, or commentary not present in the original

Your output should be ONLY the Python code, nothing else.

## Pipeline Implementation and Documentation:

```
${pipeline_implementation}
```
""")

extract_pipeline_documentation_user_prompt_template = Template("""
You are given output from an AI agent that contains both Python code and documentation with reasoning for an Apache Beam pipeline. Extract ONLY the documentation and reasoning in Markdown format.
Task:
Extract the documentation and reasoning for the Apache Beam pipeline from the input.
Instructions:

Extract all documentation content, including:

Overview of the pipeline's purpose
Architecture and design explanations
Component breakdowns
Design decisions and reasoning
Setup instructions
Any other documentation elements present


Preserve the original structure and content of the documentation
Maintain all headings, lists, and formatting elements from the original
Do not include any Python code blocks (except small examples that are part of the documentation)
Remove any text that doesn't belong to the documentation
Do not add any new explanations or commentary not present in the original

Your output should be ONLY the documentation in Markdown format, nothing else.

## Pipeline Implementation and Documentation:

```
${pipeline_implementation}
```
""")
