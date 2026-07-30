import streamlit as st
import json
def json_to_html(json_data):
    """Convert JSON to nested HTML tables with headers for objects and arrays."""
    def render_value(value):
        """Render a value or nested structure."""
        if isinstance(value, dict):
            # Process dictionary recursively
            return json_to_html(value)
        elif isinstance(value, list):
            # Process list as a sub-table
            if all(isinstance(item, dict) for item in value):
                # List of dictionaries: Render as a table
                headers = list({key for d in value for key in d.keys()})
                rows = "".join(
                    "<tr>" + "".join(f"<td>{render_value(item.get(header, ''))}</td>" for header in headers) + "</tr>"
                    for item in value
                )
                return f"<table class='nested'><thead><tr>{''.join(f'<th>{header}</th>' for header in headers)}</tr></thead><tbody>{rows}</tbody></table>"
            else:
                # List of simple values: Render as a vertical table
                return f"<table class='nested'>{''.join(f'<tr><td>{render_value(v)}</td></tr>' for v in value)}</table>"
        else:
            # Base case: Render primitive value as a string
            return str(value)

    def render_table(data):
        """Render a dictionary as an HTML table."""
        rows = ""
        for key, value in data.items():
            rows += f"<tr><th>{key}</th><td>{render_value(value)}</td></tr>"
        return f"<table class='main'>{rows}</table>"

    return render_table(json_data)

def add_custom_css():
    """Add custom CSS for styling the tables."""
    custom_css = """
    <style>
        table {
            border-collapse: collapse;
            width: 100%;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
        }
        th {
            background-color: #f4f4f4;
            text-align: left;
        }
        .nested {
            margin: 10px 0;
        }
    </style>
    """
    st.markdown(custom_css, unsafe_allow_html=True)

def yaml_to_json(yaml_content):
    """Convert YAML content to JSON."""
    import yaml
    return yaml.safe_load(yaml_content)

def main():
    st.title("JSON/YAML to Nested Table Converter (Enhanced)")

    st.write("Upload a JSON or YAML file to convert it into a nested table-like structure with headers for nested arrays.")

    # Add custom CSS for table styling
    add_custom_css()

    # File uploader
    uploaded_file = st.file_uploader("Choose a JSON or YAML file", type=["json", "yaml", "yml"])

    if uploaded_file is not None:
        try:
            # Determine file type and load content
            if uploaded_file.name.endswith(".json"):
                json_data = json.load(uploaded_file)
            elif uploaded_file.name.endswith((".yaml", ".yml")):
                yaml_content = uploaded_file.read().decode("utf-8")
                json_data = yaml_to_json(yaml_content)
            else:
                st.error("Unsupported file type. Please upload a JSON or YAML file.")
                return

            # Convert JSON to nested HTML
            html_output = json_to_html(json_data)

            # Display in Streamlit
            st.write("### Nested Table")
            st.markdown(html_output, unsafe_allow_html=True)

            # Option to download the HTML table
            st.download_button(
                label="Download HTML Table",
                data=html_output,
                file_name="nested_table.html",
                mime="text/html"
            )

            # Option to download the JSON file
            st.download_button(
                label="Download JSON File",
                data=json.dumps(json_data, indent=4),
                file_name="converted.json",
                mime="application/json"
            )

        except json.JSONDecodeError:
            st.error("Invalid JSON file. Please upload a valid JSON file.")
        except Exception as e:
            st.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
