import streamlit as st
import pandas as pd
import json
import re
from collections import defaultdict
from nbformat.v4 import new_notebook, new_code_cell
from nbformat import write

st.set_page_config(page_title="Power BI to Databricks Semantic Generator", layout="wide")
st.title("Power BI to Databricks Semantic Generator (JSON Import Mode)")

st.markdown("""
This app generates a Databricks-compatible semantic layer from a Power BI report's semantic model (exported as JSON).

**How to export the semantic model from Power BI Desktop:**
1. Open your report in Power BI Desktop.
2. Go to `File` → `Export` → `Power BI template` (`.pbit`).
3. Rename the exported `.pbit` file to `.zip` and extract it.
4. Inside the extracted folder, locate the `DataModelSchema` or `DataModel` JSON file.
5. Upload the JSON file below.
""")

uploaded_file = st.file_uploader("Upload your Power BI semantic model JSON file", type=["json"])

materialize = st.checkbox("Materialize as Databricks SQL views", value=True)
catalog_name = st.text_input("Databricks Catalog Name", value="main")
schema_name = st.text_input("Databricks Schema Name", value="semantic")
output_notebook = st.text_input("Output Notebook Filename", value="generated_semantic_notebook.ipynb")

if uploaded_file:
    pbi_model = json.load(uploaded_file)
    model = defaultdict(list)
    for table in pbi_model.get('model', {}).get('tables', []):
        table_name = table['name']
        for col in table.get('columns', []):
            model['columns'].append({'TABLE_NAME': table_name, 'COLUMN_NAME': col['name']})
            if col.get('isCalculated', False):
                model['calculated_columns'].append({'TABLE': table_name, 'NAME': col['name'], 'EXPRESSION': col.get('expression', '')})
        for measure in table.get('measures', []):
            model['measures'].append({'MEASURE_NAME': measure['name'], 'EXPRESSION': measure['expression'], 'MEASUREGROUP_NAME': table_name})
        for hierarchy in table.get('hierarchies', []):
            model['hierarchies'].append({'TABLE_NAME': table_name, 'HIERARCHY_NAME': hierarchy['name'], 'LEVELS': [lvl['name'] for lvl in hierarchy.get('levels', [])]})
    for rel in pbi_model.get('model', {}).get('relationships', []):
        model['relationships'].append(rel)

    def translate_dax_advanced(dax_expression, fallback_to_pyspark=False):
        dax_expression = re.sub(r'//.*?$', '', dax_expression, flags=re.MULTILINE)
        translations = {
            r'SUM\\s*\\(\\s*([^)]+)\\s*\\)': r'SUM(\1)',
            r'AVERAGE\\s*\\(\\s*([^)]+)\\s*\\)': r'AVG(\1)',
            r'COUNT\\s*\\(\\s*([^)]+)\\s*\\)': r'COUNT(\1)',
            r'DISTINCTCOUNT\\s*\\(\\s*([^)]+)\\s*\\)': r'COUNT(DISTINCT \1)',
            r'MIN\\s*\\(\\s*([^)]+)\\s*\\)': r'MIN(\1)',
            r'MAX\\s*\\(\\s*([^)]+)\\s*\\)': r'MAX(\1)',
            r'IF\\s*\\(\\s*([^,]+),\\s*([^,]+),\\s*([^)]+)\\s*\\)': r'CASE WHEN \1 THEN \2 ELSE \3 END',
            r'AND\\s*\\(\\s*([^,]+),\\s*([^)]+)\\s*\\)': r'(\1 AND \2)',
            r'OR\\s*\\(\\s*([^,]+),\\s*([^)]+)\\s*\\)': r'(\1 OR \2)',
            r'NOT\\s*\\(\\s*([^)]+)\\s*\\)': r'NOT (\1)',
            r'YEAR\\s*\\(\\s*([^)]+)\\s*\\)': r'EXTRACT(YEAR FROM \1)',
            r'MONTH\\s*\\(\\s*([^)]+)\\s*\\)': r'EXTRACT(MONTH FROM \1)',
            r'DAY\\s*\\(\\s*([^)]+)\\s*\\)': r'EXTRACT(DAY FROM \1)',
            r'QUARTER\\s*\\(\\s*([^)]+)\\s*\\)': r'EXTRACT(QUARTER FROM \1)',
            r'CONCATENATE\\s*\\(\\s*([^,]+),\\s*([^)]+)\\s*\\)': r'CONCAT(\1, \2)',
            r'LEFT\\s*\\(\\s*([^,]+),\\s*([^)]+)\\s*\\)': r'SUBSTRING(\1, 1, \2)',
            r'RIGHT\\s*\\(\\s*([^,]+),\\s*([^)]+)\\s*\\)': r'SUBSTRING(\1, -\2)',
            r'LEN\\s*\\(\\s*([^)]+)\\s*\\)': r'LENGTH(\1)',
            r'CALCULATE\\s*\\(\\s*([^,]+),\\s*([^)]+)\\s*\\)': r'\1 FILTER BY \2',
            r'FILTER\\s*\\(\\s*([^,]+),\\s*([^)]+)\\s*\\)': r'\1 WHERE \2',
            r'SAMEPERIODLASTYEAR\\s*\\(\\s*([^)]+)\\s*\\)': r'DATE_SUB(\1, INTERVAL 1 YEAR)',
            r'DATEADD\\s*\\(\\s*([^,]+),\\s*([^,]+),\\s*([^)]+)\\s*\\)': r'DATE_ADD(\3, INTERVAL \2 \1)',
            r'DATEDIFF\\s*\\(\\s*([^,]+),\\s*([^,]+),\\s*([^)]+)\\s*\\)': r'DATEDIFF(\1, \2, \3)',
            r'LOOKUPVALUE\\s*\\(([^)]+)\\)': r'/* LOOKUPVALUE: \1 */',
            r'RELATED\\s*\\(\\s*([^)]+)\\s*\\)': r'\1',
        }
        result = dax_expression
        for pattern, replacement in translations.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        result = re.sub(r'(\w+)\[(\w+)\]', r'\1.\2', result)
        if 'VAR' in result:
            result = re.sub(r'VAR\s+(\w+)\s*=\s*([^R]+)RETURN\s+([^$]+)', r'/* Using: \1 = \2 */ \3', result)
        if fallback_to_pyspark:
            return f"# PySpark fallback required\n# {dax_expression}\n# Please implement as needed."
        return result

    def is_sql_compatible(dax_expression):
        unsupported = ['GENERATE', 'SUMMARIZE', 'ADDCOLUMNS', 'SELECTCOLUMNS', 'UNION', 'INTERSECT', 'EXCEPT', 'EARLIER', 'PATH', 'PATHITEM']
        return not any(fn in dax_expression.upper() for fn in unsupported)

    df_measures = pd.DataFrame(model['measures'])[['MEASURE_NAME', 'EXPRESSION', 'MEASUREGROUP_NAME']]
    df_columns = pd.DataFrame(model['columns'])[['TABLE_NAME', 'COLUMN_NAME']]
    df_calc_cols = pd.DataFrame(model['calculated_columns']) if 'calculated_columns' in model else pd.DataFrame()

    st.success("Semantic model loaded. Ready to generate code.")
    if st.button("Generate Databricks Notebook"):        
        nb_out = new_notebook()
        cells_out = []
        cells_out.append(new_code_cell("""# Auto-generated Semantic Layer for Databricks\nimport pyspark.sql.functions as F\n"""))
        for table in df_columns['TABLE_NAME'].unique():
            cols = df_columns[df_columns['TABLE_NAME'] == table]['COLUMN_NAME'].tolist()
            meas = df_measures[df_measures['MEASUREGROUP_NAME'] == table]
            calc_cols = df_calc_cols[df_calc_cols['TABLE'] == table] if not df_calc_cols.empty else pd.DataFrame()
            code = f"-- Semantic View for: {table}\n-- Columns: {', '.join(cols)}\n"
            if materialize:
                code += f"CREATE OR REPLACE VIEW {catalog_name}.{schema_name}.{table}_semantic AS\nSELECT\n"
                code += ",\n".join([f"    {c}" for c in cols])
                for _, row in calc_cols.iterrows():
                    expr = row['EXPRESSION']
                    if is_sql_compatible(expr):
                        translated = translate_dax_advanced(expr)
                        code += f",\n    {translated} AS {row['NAME']}"
                    else:
                        code += f",\n    /* PySpark required for {row['NAME']} */"
                for _, row in meas.iterrows():
                    expr = row['EXPRESSION']
                    if is_sql_compatible(expr):
                        translated = translate_dax_advanced(expr)
                        code += f",\n    {translated} AS {row['MEASURE_NAME']}"
                    else:
                        code += f",\n    /* PySpark required for {row['MEASURE_NAME']} */"
                code += f"\nFROM {catalog_name}.raw.{table};"
            else:
                for _, row in calc_cols.iterrows():
                    expr = row['EXPRESSION']
                    if is_sql_compatible(expr):
                        translated = translate_dax_advanced(expr)
                        code += f"# {row['NAME']} = {translated}\n"
                    else:
                        code += f"# {row['NAME']} requires PySpark implementation\n"
                for _, row in meas.iterrows():
                    expr = row['EXPRESSION']
                    if is_sql_compatible(expr):
                        translated = translate_dax_advanced(expr)
                        code += f"# {row['MEASURE_NAME']} = {translated}\n"
                    else:
                        code += f"# {row['MEASURE_NAME']} requires PySpark implementation\n"
            cells_out.append(new_code_cell(code))
        nb_out['cells'] = cells_out
        with open(output_notebook, "w") as f:
            write(nb_out, f)
        st.success(f"Notebook '{output_notebook}' generated and saved.")
        with open(output_notebook, "rb") as f:
            st.download_button("Download Generated Notebook", f, file_name=output_notebook, mime="application/x-ipynb+json")
else:
    st.info("Please upload a Power BI semantic model JSON file to begin.")
