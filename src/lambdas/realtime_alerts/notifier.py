"""Send alerts to people through SNS (email subscriptions for Purchasing and Quality).

Why a separate module:
    The rest of the Lambda only knows "send this Alert". Where it goes (SNS email today, perhaps
    Slack or a ticketing system tomorrow) is decided here, in one place.
"""

from __future__ import annotations

from typing import Any

from src.common import config
from src.lambdas.realtime_alerts.rules import Alert

# Environment variable with the topic's ARN (set by Terraform)
ENV_ALERT_TOPIC_ARN = "ALERT_TOPIC_ARN"

# SNS rejects email subjects longer than 100 characters
MAX_SUBJECT_LENGTH = 100


class Notifier:
    """Publishes alerts to one SNS topic."""

    def __init__(self, client: Any, topic_arn: str) -> None:
        """Wrap an SNS client.

        Args:
            client: A boto3 SNS client (a fake or moto-backed one in tests).
            topic_arn: The alerts topic.
        """
        self.client = client
        self.topic_arn = topic_arn

    @classmethod
    def from_env(cls, client: Any) -> Notifier:
        """Build a Notifier for the topic named in the Lambda's environment."""
        return cls(client, config.get_str(ENV_ALERT_TOPIC_ARN))

    def publish(self, alert: Alert) -> str:
        """Send one alert.

        The alert kind and the plant travel as message attributes. Each SNS subscription has a filter
        policy on them (config/subscribers.yaml -> Terraform), so every person receives only the
        alerts they subscribed to, for example only LOW_STOCK, or only HIGH_REJECT_RATE at PLANT-MTY.
        Adding or removing people never requires changing this code.

        Args:
            alert: The alert to send.

        Returns:
            SNS's message id (logged, so an email can be traced back to the event that caused it).
        """
        response = self.client.publish(
            TopicArn=self.topic_arn,
            Subject=alert.subject[:MAX_SUBJECT_LENGTH],
            Message=alert.message,
            MessageAttributes={
                "kind": {"DataType": "String", "StringValue": alert.kind},
                "plant": {"DataType": "String", "StringValue": alert.metrics["plant"]},
            },
        )
        return response["MessageId"]
