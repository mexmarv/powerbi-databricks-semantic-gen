# Open Source Semantic Layer Generator: From Power BI to Databricks Notebooks with DAX → PySpark

This open-source project enables semantic modeling directly in Databricks based on your Power BI datasets.

## Key Features

- Automated View Generation: Creates SQL views for all base tables from your Power BI dataset
- DAX to PySpark Translation: Converts Power BI DAX measures into equivalent PySpark code
- Aggregate Functions:
-- Basic (SUM, COUNT, AVERAGE, MIN, MAX)
-- Statistical (VAR.P, STDEV.P)
-- Distinct aggregations (DISTINCTCOUNT)
- Time Intelligence Functions:
-- Period comparisons (DATEADD, SAMEPERIODLASTYEAR)
-- Year-to-date (DATESYTD, DATESMTD, DATESQTD)
-- Period navigation (PREVIOUSMONTH, PREVIOUSYEAR)
- Text Functions:
-- String operations (CONCATENATE, UPPER, LOWER)
-- Text manipulation (LEN, TRIM, SUBSTITUTE)
- Logical Functions:
-- Conditional (IF, SWITCH)
-- Boolean operations (AND, OR, NOT)
- Mathematical Functions:
-- Basic math (ABS, ROUND, FLOOR, CEILING)
-- Advanced calculations (POWER, SQRT)
- Filter Functions:
-- Context modification (CALCULATE, FILTER)
-- Table operations (ALL, ALLEXCEPT)
- Window Functions:
-- Rankings (RANKX, TOPN)
-- First/Last values (FIRSTNONBLANK, LASTNONBLANK)

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
