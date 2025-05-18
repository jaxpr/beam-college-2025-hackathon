import streamlit as st
import json
import os # Added to check if file exists

# --- CONFIGURATION ---
# Set the path to your JSON file here
JSON_FILE_PATH = "ui/example.json"

def main():
    st.set_page_config(layout="wide")
    st.title("Beam Pipeline Viewer")

    if not os.path.exists(JSON_FILE_PATH):
        st.error(f"Error: JSON file not found at path: {JSON_FILE_PATH}")
        st.info("Please ensure the JSON_FILE_PATH variable in the script is set correctly.")
        return

    try:
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 1. User Query
        st.subheader("User Query")
        st.text_area("", data.get("user_query", "No user query found."), height=100, disabled=True)
        st.divider()

        # 2. Data Analysis
        with st.expander("Data Analysis", expanded=False):
            st.markdown(data.get("data_analysis", "No data analysis found."))
        st.divider()

        # Display Data Source Metadata (optional, as in previous version)
        if "data_source_metadata" in data: # Only show if key exists
            with st.expander("Data Source Metadata", expanded=False):
                st.markdown(data.get("data_source_metadata"))
            st.divider()

        # 3. Beam Pipeline Requirements
        with st.expander("Beam Pipeline Requirements", expanded=False):
            st.markdown(data.get("requirements", "No requirements found."))
        st.divider()

        # 4. Beam Pipeline
        with st.expander("Beam Pipeline", expanded=False):
            st.code(data.get("pipeline_code", "# No pipeline code found."), language="python")
        st.divider()

        # 5. Documentation
        with st.expander("Documentation", expanded=False):
            st.markdown(data.get("pipeline_documentation", "No documentation found."))

    except FileNotFoundError: # This is now handled by the os.path.exists check, but good to keep
        st.error(f"Error: The file was not found at the specified path: {JSON_FILE_PATH}")
    except json.JSONDecodeError:
        st.error(f"Error: Invalid JSON file. Please check the format of the file at: {JSON_FILE_PATH}")
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
