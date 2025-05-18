from google import genai
from google.genai import types

from agents.prompts.data_engineer import pipeline_generation_system_prompt_template, \
    pipeline_generation_user_prompt_template, extract_pipeline_code_user_prompt_template, \
    extract_pipeline_documentation_user_prompt_template


class DataEngineerAgent:
    DEFAULT_MODEL_NAME = "gemini-2.5-pro-preview-05-06"
    FORMATTING_MODEL_NAME = "gemini-2.5-flash-preview-04-17"  # Model for code/doc refinement

    def __init__(self, state: dict, client: genai.Client):
        self.state = state
        self.client = client

    def _generate_llm_response(self, user_prompt: str, model_name: str,
                               system_prompt: str = None) -> str:
        """
        Helper method to generate text content using the configured genai client.
        """
        processed_user_prompt = [user_prompt] if isinstance(user_prompt, str) else user_prompt

        llm_call_config_arg = None

        if system_prompt:
            try:
                llm_call_config_arg = types.GenerateContentConfig(system_instruction=system_prompt)
            except TypeError as e:
                print(
                    f"Warning: Failed to set system_instruction via types.GenerateContentConfig for model {model_name}: {e}.")

        response = self.client.models.generate_content(
            model=model_name,
            contents=processed_user_prompt,
            config=llm_call_config_arg
        )
        return response.text

    def _generate_initial_pipeline_implementation(self, user_query: str, data_source_metadata: str,
                                                  requirements: str, output_bucket: str) -> str:
        """
        Generates the initial (raw) pipeline code using the primary LLM.
        """
        system_prompt = pipeline_generation_system_prompt_template.safe_substitute()
        user_prompt = pipeline_generation_user_prompt_template.safe_substitute(
            user_query=user_query,
            data_source_metadata=data_source_metadata,
            requirements=requirements,
            output_bucket=output_bucket
        )

        pipeline_implementation = self._generate_llm_response(
            user_prompt=user_prompt,
            system_prompt=system_prompt,
            model_name=self.DEFAULT_MODEL_NAME,
        )
        return pipeline_implementation

    def _extract_pipeline_code(self, raw_pipeline_implementation: str) -> str:
        """
        Uses an LLM call to extract clean pipeline code from the raw implementation.
        Assumes the LLM (with the updated prompt) now generates the code string directly
        without needing specific markdown fence stripping.
        """
        code_extraction_system_prompt = None
        code_extraction_prompt = extract_pipeline_code_user_prompt_template.safe_substitute(
            pipeline_implementation=raw_pipeline_implementation
        )

        clean_code = self._generate_llm_response(
            user_prompt=code_extraction_prompt,
            system_prompt=code_extraction_system_prompt,
            model_name=self.FORMATTING_MODEL_NAME
        )
        # Removed specific markdown stripping (e.g., "```python").
        # .strip() is kept for minimal leading/trailing whitespace cleanup,
        # which is generally safe. If your LLM output is perfectly clean
        # (no leading/trailing whitespace at all), this could also be removed.
        return clean_code.strip()

    def _generate_pipeline_documentation(self, clean_pipeline_code: str) -> str:
        """
        Uses an LLM call to generate documentation for the provided clean pipeline code.
        Assumes the LLM (with the updated prompt) now generates the documentation string directly.
        """
        doc_generation_system_prompt = None
        doc_generation_prompt = extract_pipeline_documentation_user_prompt_template.safe_substitute(
            pipeline_code=clean_pipeline_code
        )

        documentation = self._generate_llm_response(
            user_prompt=doc_generation_prompt,
            system_prompt=doc_generation_system_prompt,
            model_name=self.FORMATTING_MODEL_NAME
        )
        # .strip() is kept for minimal leading/trailing whitespace cleanup.
        return documentation.strip()

    def _pipeline_code_post_processing(self, pipeline_code: str) -> str:
        """
        Simple post-processing: remove potential markdown backticks if LLM adds them automatically.
        """
        if pipeline_code.startswith("```python\n"):
            pipeline_code = pipeline_code[len("```python\n"):]
        if pipeline_code.startswith("```\n"):
            pipeline_code = pipeline_code[len("```\n"):]
        if pipeline_code.endswith("\n```"):
            pipeline_code = pipeline_code[:-len("\n```")]
        return pipeline_code.strip()

    def generate(self) -> tuple[str, str]:
        """
        Generates data processing pipeline code and its documentation.

        This involves a three-step LLM process:
        1. Generate initial raw pipeline code.
        2. Extract the code from the raw output using a dedicated LLM call.
        3. Generate documentation based on the extracted code using another dedicated LLM call.

        Raises:
            KeyError: If "user_query", "data_source_metadata", or "requirements"
                      are not found in the agent's state.

        Returns:
            tuple[str, str]: A tuple containing:
                - pipeline_code (str): The generated Apache Beam pipeline code.
                - pipeline_documentation (str): The documentation for the pipeline.
        """
        try:
            user_query = self.state["user_query"]
            data_source_metadata = self.state["data_source_metadata"]
            requirements = self.state["requirements"]
            output_bucket = self.state["output_bucket"]
        except KeyError as e:
            raise KeyError(f"Missing required key in agent state: {e}. ") from e

        # Step 1: Generate initial (raw) pipeline implementation
        raw_pipeline_implementation = self._generate_initial_pipeline_implementation(
            user_query, data_source_metadata, requirements, output_bucket
        )

        # Step 2: Extract the code from the raw output
        # Assumes this call now returns a clean code string due to improved prompts
        pipeline_code = self._extract_pipeline_code(raw_pipeline_implementation)
        pipeline_code = self._pipeline_code_post_processing(pipeline_code)

        # Step 3: Generate documentation based on the extracted code
        # Assumes this call now returns a clean documentation string
        pipeline_documentation = self._generate_pipeline_documentation(pipeline_code)

        return pipeline_code, pipeline_documentation
