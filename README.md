# Semantic Layer Generator: From Power BI to Databricks Notebooks with DAX → PySpark

This open-source project enables semantic modeling directly in Databricks based on your Power BI datasets.

## Key Features

- Automated View Generation: Creates SQL views for all base tables from your Power BI dataset
- DAX to PySpark Translation: Converts Power BI DAX measures into equivalent PySpark code
- Aggregate Functions:
 - Basic (SUM, COUNT, AVERAGE, MIN, MAX)
 - Statistical (VAR.P, STDEV.P)
 - Distinct aggregations (DISTINCTCOUNT)
- Time Intelligence Functions:
 - Period comparisons (DATEADD, SAMEPERIODLASTYEAR)
 - Year-to-date (DATESYTD, DATESMTD, DATESQTD)
 - Period navigation (PREVIOUSMONTH, PREVIOUSYEAR)
- Text Functions:
 - String operations (CONCATENATE, UPPER, LOWER)
 - Text manipulation (LEN, TRIM, SUBSTITUTE)
- Logical Functions:
 - Conditional (IF, SWITCH)
 - Boolean operations (AND, OR, NOT)
- Mathematical Functions:
 - Basic math (ABS, ROUND, FLOOR, CEILING)
 - Advanced calculations (POWER, SQRT)
- Filter Functions:
 - Context modification (CALCULATE, FILTER)
 - Table operations (ALL, ALLEXCEPT)
- Window Functions:
 - Rankings (RANKX, TOPN)
 - First/Last values (FIRSTNONBLANK, LASTNONBLANK)

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

## Explanation

The conversion process we developed takes your Power BI DAX semantic model and translates its logic into Python (using PySpark) to work within Databricks—but it does so without a built‐in OLAP engine. 
Here’s how that works in practice:

- Semantic Model as a Set of Transformations: Instead of an in-memory cube (as Power BI’s VertiPaq engine provides), the semantic model is reimagined as a series of transformation steps (or views) that operate on data stored in dimensional tables (fact and dimensions). In our conversion, each DAX measure, calculated column, or relationship is converted into a PySpark expression or SQL query that processes these tables.
- Dimensional Modeling on Delta Lake: Although Databricks doesn’t have an OLAP engine, you can still implement dimensional models by designing your data into star or snowflake schemas: With this structure in place, your transformed DAX logic is applied directly over these tables via Spark SQL or PySpark DataFrame operations.
- Fact Tables contain the raw quantitative data.
- Dimension Tables provide context (such as dates, products, or geographies).

These structures are physically stored in Delta Lake tables, which support ACID transactions and efficient queries.

Translation of DAX Expressions to PySpark: In Power BI, DAX expressions are evaluated on an in-memory OLAP engine optimized for aggregation and filtering. 

When converting to Python in Databricks:
- Aggregation, Filtering, and Conditional Logic: The DAX expressions are mapped to PySpark functions (like groupBy().agg(), filter(), and withColumn()), which compute the same aggregates across distributed data.
- Handling Context and Relationships:While OLAP engines automatically manage the multidimensional context (like slicers or complex relationship joins), in Databricks you explicitly define the transformation logic in your notebook. This might mean writing explicit joins between fact and dimension tables or materializing intermediate views to simulate the context behavior.
- Performance Considerations: Since there is no native caching or indexing provided by an OLAP engine, you might need to leverage caching, Delta table optimizations, or scheduled materializations to achieve interactive performance similar to Power BI’s in-memory engine.

Building the Pipeline: The generated pipeline notebook effectively becomes your “semantic layer” in Databricks:
- Modular Cells for Each Transformation: Each notebook cell corresponds to a specific transformation or measure extracted from the original DAX semantic model.
- Materialization Steps: If enabled, additional steps materialize the output into Delta tables. This serves a similar purpose to an OLAP cube by precomputing aggregates, which can be queried quickly by downstream applications (including Power BI via DirectQuery or other BI tools).
End-to-End Workflow: Your data is ingested, transformed according to the translated DAX logic, and written out to structures that are optimized for large-scale querying. This forms a scalable, distributed pipeline that effectively replaces the OLAP engine with Spark’s distributed processing capabilities.

In Summary
- No Native OLAP Engine: Databricks does not provide an in-memory cube engine. Instead, you organize your data into well-structured Delta Lake tables following a dimensional model.
- Transformation via Spark: The conversion process translates DAX calculations into PySpark code, which is executed across a distributed Spark cluster. This code replicates the logic of DAX but runs on your transformed data (fact and dimensions).
- Achieving OLAP-Like Results: By precomputing views, materializing aggregations, and designing your pipeline with star schemas, you simulate the benefits of a multidimensional cube. While the underlying technology is different, you still enable robust and scalable analytical processing on large datasets.

In essence, the conversion leverages Spark’s scalable processing power and the flexibility of Delta Lake to implement the same business logic that an OLAP engine would manage, but in a distributed, code-driven way rather than through a dedicated multidimensional engine.

## License

MIT License (see LICENSE file).

Developed by Marvin Nahmias.
