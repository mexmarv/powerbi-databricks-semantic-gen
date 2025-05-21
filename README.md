# Power BI to Databricks Semantic Generator

This project provides a notebook that automatically extracts the semantic model from a Power BI report (exported as a JSON file), translates DAX measures and calculated columns to Databricks SQL or PySpark, and generates materialized views in Unity Catalog if desired.

## Features

- Extracts tables, columns, measures, calculated columns, hierarchies, and relationships from a Power BI semantic model JSON
- Translates DAX to Databricks SQL for most aggregation, logical, time intelligence, and text functions
- Falls back to PySpark for unsupported or complex DAX logic
- Optionally materializes metrics and dimensions as Databricks SQL views
- Outputs a Databricks-importable notebook

## How to Export the Semantic Model from Power BI Desktop

1. Open your report in Power BI Desktop.
2. Go to `File` → `Export` → `Power BI template` (`.pbit`).
3. Rename the exported `.pbit` file to `.zip` and extract it.
4. Inside the extracted folder, locate the `DataModelSchema` or `DataModel` JSON file (usually named `DataModelSchema` or `DataModel`).
5. Place this JSON file in your working directory and set the `PBI_JSON_PATH` variable in the notebook to its filename.

## Usage

1. Install the required Python packages:

   ```sh
   pip install pandas nbformat
   ```

2. Open the notebook `semantic_generator_v11_beastmode_ULTRA_FULL.ipynb` in VS Code or Databricks.
3. Set the `PBI_JSON_PATH` variable in the configuration cell to the name of your exported JSON file.
4. Run all cells. The notebook will generate a new Databricks notebook with SQL and PySpark code for your semantic layer.

## Output

- The generated notebook will contain Databricks SQL code to create views for each table, including all columns, calculated columns, and measures.
- If a DAX expression cannot be translated to SQL, a PySpark code comment will be included for manual implementation.

## License

MIT License
