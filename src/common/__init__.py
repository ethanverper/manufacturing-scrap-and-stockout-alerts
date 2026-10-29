"""Shared helpers used by every component of the pipeline.

- config.py         reads settings from environment variables, with safe defaults
- logging_utils.py  structured JSON logging for CloudWatch Logs Insights
- aws.py            one place to create AWS clients, so tests can replace them
"""
