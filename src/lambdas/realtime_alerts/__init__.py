"""Real-time alerts Lambda: reads plant events from Kinesis and emails Purchasing and Quality.

Modules (thin handler, pure logic, isolated AWS access):
    parsing.py   decode and validate each record -> PlantEvent        (level 2, pure)
    rules.py     stock coverage, reject rate, alert decisions        (level 2, pure)
    state.py     DynamoDB: idempotent transaction, reads, alert flags (level 3)
    notifier.py  SNS email                                            (level 3)
    handler.py   the Lambda entry point that wires them together      (level 3)
"""
