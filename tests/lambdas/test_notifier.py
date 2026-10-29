"""SNS publishing: the message carries the attributes subscription filters depend on."""

from __future__ import annotations

import json

import boto3

from src.lambdas.realtime_alerts.notifier import Notifier
from src.lambdas.realtime_alerts.rules import LOW_STOCK, Alert


def test_alert_reaches_only_matching_subscribers(aws_env: None) -> None:
    """Two queues subscribed with different filter policies: only the matching one gets the alert.

    SQS queues stand in for email inboxes (moto can't read emails), but the filter logic is the same.
    """
    sns = boto3.client("sns", region_name="us-east-1")
    sqs = boto3.client("sqs", region_name="us-east-1")
    topic_arn = sns.create_topic(Name="alerts")["TopicArn"]

    # One "inbox" per subscriber, each with the filter policy Terraform would create
    inboxes = {}
    for name, policy in {
        "purchasing": {"kind": ["LOW_STOCK"]},
        "quality_mty": {"kind": ["HIGH_REJECT_RATE"], "plant": ["PLANT-MTY"]},
    }.items():
        url = sqs.create_queue(QueueName=name)["QueueUrl"]
        arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=arn, Attributes={"FilterPolicy": json.dumps(policy)})
        inboxes[name] = url

    # Publish a low-stock alert for Monterrey
    alert = Alert(kind=LOW_STOCK, dedupe_key="k", subject="Low stock", message="m", metrics={"plant": "PLANT-MTY"})
    Notifier(sns, topic_arn).publish(alert)

    # Purchasing receives it; Quality doesn't (wrong kind)
    received = {name: sqs.receive_message(QueueUrl=url).get("Messages", []) for name, url in inboxes.items()}
    assert len(received["purchasing"]) == 1
    assert received["quality_mty"] == []
