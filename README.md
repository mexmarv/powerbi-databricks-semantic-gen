# Power BI to Databricks Semantic Layer Generator

This open-source project enables semantic modeling directly in Databricks based on your Power BI datasets.

## Key Features

- Extracts tables, relationships, and DAX measures from Power BI via REST API
- Auto-generates:
  - SQL views for base and joined tables
  - PySpark translations of DAX measures (SUM, CALCULATE, FILTER, DIVIDE, etc.)
  - Optional Delta materialized tables (`materialize = True`)
- Adds editable blocks to define business rules (semantic logic)
- Designed for use within Databricks Notebooks

## Example Use Case

Imagine you have a Power BI model with `FactSales` and `DimCustomer` tables. This generator:

- Reads that model using Power BI REST API
- Builds `semantic.FactSales`, `semantic.vw_FactSales` (with JOIN to customers)
- Translates measures like:
  - `CALCULATE(SUM(FactSales[Amount]), FILTER(...))`
  - `DISTINCTCOUNT(FactSales[CustomerKey])`
- Inserts example PySpark logic for editable custom rules

## How to Use

1. Register a Power BI app in Azure to get:
   - `client_id`, `client_secret`, `tenant_id`, `workspace_id`, `dataset_id`
2. Open `semantic_generator_v9_EN.ipynb` in Databricks
3. Set your parameters and run the notebook
4. The notebook will generate `semantic_<your_model>.ipynb`
5. Open that notebook to validate or extend your semantic layer

## License

MIT License (see LICENSE file).

Developed by Marvin Nahmias.
