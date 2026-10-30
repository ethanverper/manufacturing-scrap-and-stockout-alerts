"""Analytics layer: run the KPI queries in sql/ (on Athena or a local Spark) and draw the README charts.

- queries.py      finds and reads the .sql files
- athena.py       runs a query in the Athena workgroup and returns plain rows
- local_spark.py  runs the same SQL with Spark on the locally built lake (no AWS)
- charts.py       turns query rows into PNG charts (pure: rows in, figure out)
"""
