"""Shared helpers used by every component of the pipeline.

- settings.py       reads config/settings.yaml and config/master_data.yaml (local code)
- config.py         reads settings from environment variables, with safe defaults (code in AWS)
- logging_utils.py  structured JSON logging for CloudWatch Logs Insights
- aws.py            one place to create AWS clients, so tests can replace them
"""
