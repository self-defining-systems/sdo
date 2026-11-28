"""CloudWatch user-journey alarms that page an AWS DevOps Agent Space, fed by a lane's FuzzOps user traffic.

The baseline operator's own monitoring of the app's users, on the lane host:

``publish``
    Tails the per-request journey outcomes FuzzOps appends to ``$FUZZOPS_JOURNEY_OUTCOMES`` (one JSON line per
    request: start, journey, ok, latency; nothing else) and puts per-journey 10 s ``Requests``, ``Failed`` and
    ``Latency`` metrics into CloudWatch.
``render`` / ``apply`` / ``teardown``
    One alarm per journey on its failure ratio, a default-bus EventBridge rule on those alarms' ALARM transitions and
    the SQS queue it delivers to.
``relay``
    Reads the queue and sends each ALARM transition once to the Agent Space's HMAC generic webhook. The incident id is
    sha256(alarm ARN, transition time), so a replayed delivery is the same incident; the relay's ledger also skips it.

Nothing on these paths names a fault, a schedule position or a grade: metric names, dimensions, alarm descriptions
and webhook payloads carry only the lane, the journey and CloudWatch's own alarm fields.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import re
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

NAMESPACE = "UserJourneys"
ALARM_EVENT = "CloudWatch Alarm State Change"
#: A bucket is published once every request that started in it has had time to finish (FuzzOps' 3 s request timeout,
#: plus margin).
CLOSE_AFTER_SECONDS = 5.0
_NAME = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_WEBHOOK = re.compile(r"https://event-ai\.[a-z0-9-]+\.api\.aws/webhook/generic/[A-Za-z0-9_-]+")


@dataclass(frozen=True)
class AlarmPolicy:
    """A journey's alarm: failure ratio >= threshold in `datapoints` of the last `evaluation_periods` periods.

    A period without requests has no datapoint (the ratio is undefined), so it neither breaches nor clears.
    """

    period_seconds: int = 10
    threshold: float = 0.2
    evaluation_periods: int = 3
    datapoints: int = 2

    def __post_init__(self) -> None:
        if self.period_seconds not in (10, 30) and (self.period_seconds < 60 or self.period_seconds % 60):
            raise ValueError("period_seconds must be 10, 30 or a multiple of 60")
        if not 0 < self.threshold <= 1:
            raise ValueError("threshold is a failure ratio in (0, 1]")
        if not 1 <= self.datapoints <= self.evaluation_periods:
            raise ValueError("need 1 <= datapoints <= evaluation_periods")


@dataclass(frozen=True)
class Config:
    account: str
    region: str
    lane: str
    journeys: dict[str, AlarmPolicy]
    queue: str = ""

    def __post_init__(self) -> None:
        if not re.fullmatch(r"\d{12}", self.account):
            raise ValueError("account must be a 12-digit AWS account id")
        if not re.fullmatch(r"[a-z]{2}(-[a-z]+)+-\d", self.region):
            raise ValueError(f"invalid region {self.region!r}")
        if not self.journeys:
            raise ValueError("at least one journey is required")
        for name in (self.lane, *self.journeys):
            if not _NAME.fullmatch(name):
                raise ValueError(f"invalid lane or journey name {name!r}")
        if any(not isinstance(p, AlarmPolicy) for p in self.journeys.values()):
            raise TypeError("journeys maps names to AlarmPolicy")

    @classmethod
    def load(cls, data: Mapping[str, Any]) -> Config:
        default = data.get("default_policy", {})
        overrides = data.get("policies", {})
        journeys = {j: AlarmPolicy(**{**default, **overrides.get(j, {})}) for j in data["journeys"]}
        unknown = set(overrides) - set(journeys)
        if unknown:
            raise ValueError(f"policies for unknown journeys: {sorted(unknown)}")
        return cls(data["account"], data["region"], data["lane"], journeys, data.get("queue", ""))

    @property
    def queue_name(self) -> str:
        return self.queue or f"{self.lane}-journey-alarms"

    @property
    def rule_name(self) -> str:
        return f"{self.lane}-journey-alarms"

    def alarm_name(self, journey: str) -> str:
        return f"{self.lane}-journey-{journey}"

    def alarm_arn(self, journey: str) -> str:
        return f"arn:aws:cloudwatch:{self.region}:{self.account}:alarm:{self.alarm_name(journey)}"

    def dimensions(self, journey: str) -> list[dict[str, str]]:
        return [{"Name": "Lane", "Value": self.lane}, {"Name": "Journey", "Value": journey}]


def render(config: Config) -> dict[str, Any]:
    """PutMetricAlarm arguments per journey and the EventBridge pattern of their ALARM transitions."""
    alarms = []
    for journey, policy in config.journeys.items():

        def stat(metric: str, ident: str, journey: str = journey, policy: AlarmPolicy = policy) -> dict[str, Any]:
            return {
                "Id": ident,
                "MetricStat": {
                    "Metric": {"Namespace": NAMESPACE, "MetricName": metric, "Dimensions": config.dimensions(journey)},
                    "Period": policy.period_seconds,
                    "Stat": "Sum",
                },
                "ReturnData": False,
            }

        alarms.append(
            {
                "AlarmName": config.alarm_name(journey),
                "AlarmDescription": (
                    f"Users of journey {journey} on {config.lane}: at least {policy.threshold:.0%} of requests failed "
                    f"in {policy.datapoints} of {policy.evaluation_periods} {policy.period_seconds}-second periods."
                ),
                "Metrics": [
                    stat("Failed", "failed"),
                    stat("Requests", "requests"),
                    {
                        "Id": "ratio",
                        "Expression": "IF(requests > 0, failed / requests)",
                        "Label": f"{journey} failure ratio",
                        "ReturnData": True,
                    },
                ],
                "Threshold": policy.threshold,
                "ComparisonOperator": "GreaterThanOrEqualToThreshold",
                "EvaluationPeriods": policy.evaluation_periods,
                "DatapointsToAlarm": policy.datapoints,
                "TreatMissingData": "missing",
                "Tags": [{"Key": "Lane", "Value": config.lane}],
            }
        )
    pattern = {
        "source": ["aws.cloudwatch"],
        "detail-type": [ALARM_EVENT],
        "account": [config.account],
        "region": [config.region],
        "resources": [config.alarm_arn(journey) for journey in config.journeys],
        "detail": {"state": {"value": ["ALARM", "OK"]}},
    }
    return {"alarms": alarms, "rule": {"Name": config.rule_name, "EventPattern": pattern}, "queue": config.queue_name}


# --- metrics ---------------------------------------------------------------------------------------------------


def parse_time(value: str) -> datetime:
    """An ISO time with a zone (Go's RFC 3339 nanoseconds included), in UTC."""
    text = value.replace("Z", "+00:00")
    if match := re.fullmatch(r"(.*T\d\d:\d\d:\d\d)\.(\d+)([+-]\d\d:\d\d)", text):
        text = f"{match[1]}.{match[2][:6].ljust(6, '0')}{match[3]}"
    stamp = datetime.fromisoformat(text)
    if stamp.tzinfo is None:
        raise ValueError("timestamp requires a time zone")
    return stamp.astimezone(timezone.utc)


@dataclass
class Bucket:
    requests: int = 0
    failed: int = 0
    latencies: list[int] = field(default_factory=list)


@dataclass
class Aggregator:
    """Per-journey counts of requests by the period their request started in."""

    config: Config
    period: int = 10
    buckets: dict[int, dict[str, Bucket]] = field(default_factory=dict)

    def add(self, line: str) -> None:
        try:
            outcome = json.loads(line)
            journey, ok, latency = outcome["journey"], outcome["ok"], int(outcome["latency_ms"])
            start = parse_time(outcome["at"]).timestamp()
        except (ValueError, KeyError, TypeError):
            return
        if journey not in self.config.journeys or not isinstance(ok, bool):
            return
        slot = int(start // self.period) * self.period
        bucket = self.buckets.setdefault(slot, {}).setdefault(journey, Bucket())
        bucket.requests += 1
        bucket.failed += not ok
        bucket.latencies.append(max(latency, 0))

    def closed(self, now: float) -> list[dict[str, Any]]:
        """MetricData of every period closed by `now`, removed from the aggregator.

        A period with any traffic gets a datapoint for every configured journey: a journey that sent no request then
        reads 0 requests (no ratio), which says the traffic ran, unlike a period with no traffic at all.
        """
        data: list[dict[str, Any]] = []
        for slot in sorted(s for s in self.buckets if s + self.period + CLOSE_AFTER_SECONDS <= now):
            seen = self.buckets.pop(slot)
            stamp = datetime.fromtimestamp(slot, timezone.utc).isoformat()
            for journey in self.config.journeys:
                bucket = seen.get(journey, Bucket())
                dims = self.config.dimensions(journey)
                for name, value in (("Requests", bucket.requests), ("Failed", bucket.failed)):
                    data.append(
                        {
                            "MetricName": name,
                            "Dimensions": dims,
                            "Timestamp": stamp,
                            "Value": value,
                            "Unit": "Count",
                            "StorageResolution": 1,
                        }
                    )
                if bucket.latencies:
                    data.append(
                        {
                            "MetricName": "Latency",
                            "Dimensions": dims,
                            "Timestamp": stamp,
                            "StatisticValues": {
                                "SampleCount": len(bucket.latencies),
                                "Sum": sum(bucket.latencies),
                                "Minimum": min(bucket.latencies),
                                "Maximum": max(bucket.latencies),
                            },
                            "Unit": "Milliseconds",
                            "StorageResolution": 1,
                        }
                    )
        return data


def batches(data: Sequence[dict[str, Any]], size: int = 500) -> list[list[dict[str, Any]]]:
    """PutMetricData takes at most 1000 datums per call; stay well under its 1 MB body."""
    return [list(data[i : i + size]) for i in range(0, len(data), size)]


@dataclass
class Tail:
    """Lines appended to a file, across truncation, rotation and the file not existing yet."""

    path: Path
    offset: int = 0
    inode: int | None = None
    partial: str = ""

    def read(self) -> list[str]:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            return []
        if stat.st_ino != self.inode or stat.st_size < self.offset:
            self.inode, self.offset, self.partial = stat.st_ino, 0, ""
        with self.path.open("r", encoding="utf-8", errors="replace") as source:
            source.seek(self.offset)
            text = source.read()
            self.offset = source.tell()
        text, self.partial = self.partial + text, ""
        lines = text.split("\n")
        self.partial = lines.pop()
        return [line for line in lines if line.strip()]


# --- AWS --------------------------------------------------------------------------------------------------------


def aws_cli(region: str) -> Callable[..., dict[str, Any]]:
    def call(*args: str) -> dict[str, Any]:
        done = subprocess.run(
            ["aws", *args, "--region", region, "--output", "json", "--no-cli-pager"],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if done.returncode:
            raise RuntimeError(f"aws {args[0]} {args[1]} failed: {done.stderr.strip()[-400:]}")
        return json.loads(done.stdout) if done.stdout.strip() else {}

    return call


def publish(config: Config, outcomes: Path, aws: Callable[..., dict[str, Any]], *, every: float = 2.0) -> None:
    """Run until killed: tail the outcomes file, put each closed period's metrics."""
    tail, agg = Tail(outcomes), Aggregator(config)
    tail.read()  # outcomes from before the publisher started are history, not current users
    while True:
        for line in tail.read():
            agg.add(line)
        for batch in batches(agg.closed(time.time())):
            try:
                aws("cloudwatch", "put-metric-data", "--namespace", NAMESPACE, "--metric-data", json.dumps(batch))
            except RuntimeError as exc:
                print(f"publish: {exc}", file=sys.stderr, flush=True)
        time.sleep(every)


def apply(config: Config, aws: Callable[..., dict[str, Any]]) -> dict[str, Any]:
    """Create or update the queue, the alarms and the rule. Idempotent."""
    rendered = render(config)
    queue = aws("sqs", "create-queue", "--queue-name", config.queue_name)["QueueUrl"]
    queue_arn = aws("sqs", "get-queue-attributes", "--queue-url", queue, "--attribute-names", "QueueArn")["Attributes"][
        "QueueArn"
    ]
    rule = rendered["rule"]
    rule_arn = aws("events", "put-rule", "--name", rule["Name"], "--event-pattern", json.dumps(rule["EventPattern"]))[
        "RuleArn"
    ]
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "events.amazonaws.com"},
                "Action": "sqs:SendMessage",
                "Resource": queue_arn,
                "Condition": {"ArnEquals": {"aws:SourceArn": rule_arn}},
            }
        ],
    }
    aws(
        "sqs",
        "set-queue-attributes",
        "--queue-url",
        queue,
        "--attributes",
        json.dumps({"Policy": json.dumps(policy), "MessageRetentionPeriod": "3600"}),
    )
    target = [
        {"Id": "relay", "Arn": queue_arn, "RetryPolicy": {"MaximumRetryAttempts": 5, "MaximumEventAgeInSeconds": 300}}
    ]
    if aws("events", "put-targets", "--rule", rule["Name"], "--targets", json.dumps(target))["FailedEntryCount"]:
        raise RuntimeError("EventBridge refused the queue target")
    for alarm in rendered["alarms"]:
        aws("cloudwatch", "put-metric-alarm", "--cli-input-json", json.dumps(alarm))
    return {"queue_url": queue, "queue_arn": queue_arn, "rule_arn": rule_arn, "alarms": len(rendered["alarms"])}


def teardown(config: Config, aws: Callable[..., dict[str, Any]]) -> list[str]:
    """Remove the rule, alarms and queue (published metric samples age out under CloudWatch retention)."""
    done: list[str] = []

    def attempt(what: str, *args: str) -> None:
        try:
            aws(*args)
            done.append(what)
        except RuntimeError as exc:
            done.append(f"{what}: {exc}")

    attempt("targets", "events", "remove-targets", "--rule", config.rule_name, "--ids", "relay")
    attempt("rule", "events", "delete-rule", "--name", config.rule_name)
    names = [config.alarm_name(j) for j in config.journeys]
    for i in range(0, len(names), 100):
        attempt("alarms", "cloudwatch", "delete-alarms", "--alarm-names", *names[i : i + 100])
    try:
        url = aws("sqs", "get-queue-url", "--queue-name", config.queue_name)["QueueUrl"]
        attempt("queue", "sqs", "delete-queue", "--queue-url", url)
    except RuntimeError as exc:
        done.append(f"queue: {exc}")
    return done


# --- relay ------------------------------------------------------------------------------------------------------


def incident_id(alarm_arn: str, transition: str) -> str:
    return hashlib.sha256(f"{alarm_arn}\n{transition}".encode()).hexdigest()


def alarm_page(
    config: Config, event: Mapping[str, Any], opened: Mapping[str, str] | None = None
) -> dict[str, Any] | None:
    """The webhook payload for one of the lane's alarms changing state, else None.

    ALARM opens an incident (action ``created``; the id is the alarm ARN and the transition time). The return to OK
    resolves that same incident (action ``resolved``): an on-call integration that never says an episode is over
    makes the service treat a later episode as the same ongoing incident and link it to the first. ``opened`` maps
    an alarm ARN to the incident it has open (the relay's ledger); without it the id is derived from the ALARM
    state the OK event reports as previous. Only trusted fields are used: the alarm ARN must be one of the lane's,
    and CloudWatch's free-form reason is dropped.
    """
    if (event.get("source"), event.get("detail-type"), event.get("account"), event.get("region")) != (
        "aws.cloudwatch",
        ALARM_EVENT,
        config.account,
        config.region,
    ):
        return None
    detail = event.get("detail") or {}
    state = detail.get("state") or {}
    resources = event.get("resources")
    value = state.get("value")
    if value not in ("ALARM", "OK") or not isinstance(resources, list) or len(resources) != 1:
        return None
    for journey, policy in config.journeys.items():
        arn = config.alarm_arn(journey)
        if resources != [arn]:
            continue
        transition = str(state["timestamp"])
        metrics = f"metrics {NAMESPACE} Failed/Requests/Latency, dimensions Lane={config.lane}, Journey={journey}"
        if value == "ALARM":
            return {
                "eventType": "incident",
                "incidentId": incident_id(arn, transition),
                "action": "created",
                "priority": "HIGH",
                "title": f"User journey failing: {journey}",
                "description": (
                    f"CloudWatch alarm {config.alarm_name(journey)} entered ALARM at {transition}: at least "
                    f"{policy.threshold:.0%} of the {journey} requests of the app's users failed in "
                    f"{policy.datapoints} of {policy.evaluation_periods} {policy.period_seconds}-second periods "
                    f"({metrics})."
                ),
                "timestamp": transition,
                "service": config.lane,
            }
        previous = detail.get("previousState") or {}
        open_id = (opened or {}).get(arn)
        if open_id is None and previous.get("value") == "ALARM" and previous.get("timestamp"):
            open_id = incident_id(arn, str(previous["timestamp"]))
        if open_id is None:
            return None
        return {
            "eventType": "incident",
            "incidentId": open_id,
            "action": "resolved",
            "priority": "HIGH",
            "title": f"User journey failing: {journey}",
            "description": (
                f"CloudWatch alarm {config.alarm_name(journey)} returned to OK at {transition}: fewer than "
                f"{policy.threshold:.0%} of the {journey} requests of the app's users failed ({metrics})."
            ),
            "timestamp": transition,
            "service": config.lane,
        }
    return None


@dataclass(frozen=True)
class Webhook:
    url: str
    secret: str = field(repr=False)

    def __post_init__(self) -> None:
        if not _WEBHOOK.fullmatch(self.url):
            raise ValueError("expected the regional AWS DevOps Agent generic webhook URL")
        if not self.secret:
            raise ValueError("the webhook secret is empty")

    @classmethod
    def load(cls, path: Path) -> Webhook:
        hook = json.loads(path.read_text())["webhook"]
        if hook.get("webhookType") != "hmac":
            raise ValueError("expected an HMAC webhook")
        return cls(hook["webhookUrl"], hook["webhookSecret"])

    def headers(self, body: bytes, sent_at: str) -> dict[str, str]:
        digest = hmac.digest(self.secret.encode(), sent_at.encode() + b":" + body, "sha256")
        return {
            "Content-Type": "application/json",
            "x-amzn-event-timestamp": sent_at,
            "x-amzn-event-signature": base64.b64encode(digest).decode(),
        }


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        return None


def post(hook: Webhook, payload: Mapping[str, Any]) -> int:
    body = json.dumps(payload, separators=(",", ":")).encode()
    sent_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    request = urllib.request.Request(hook.url, data=body, headers=hook.headers(body, sent_at), method="POST")
    with urllib.request.build_opener(_NoRedirect()).open(request, timeout=15) as response:
        return int(response.status)


def ledger_state(ledger: Path) -> tuple[set[str], dict[str, str]]:
    """What the ledger says was delivered: ``<incident>:<action>`` keys, and the incident each alarm has open."""
    sent: set[str] = set()
    opened: dict[str, str] = {}
    if not ledger.is_file():
        return sent, opened
    for line in ledger.read_text().splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if record.get("status") != 200:
            continue
        action = record.get("action", "created")
        sent.add(f"{record['incident_id']}:{action}")
        if action == "created":
            opened[record.get("alarm_arn", "")] = record["incident_id"]
        else:
            opened.pop(record.get("alarm_arn", ""), None)
    return sent, opened


def relay_once(
    config: Config,
    messages: Iterable[Mapping[str, Any]],
    send: Callable[[Mapping[str, Any]], int],
    sent: set[str],
    out: IO[str],
    now: Callable[[], float] = time.time,
    opened: dict[str, str] | None = None,
) -> list[str]:
    """Send each new ALARM and OK transition once; returns the receipt handles done (sent, duplicate or foreign)."""
    opened = {} if opened is None else opened
    done = []
    for message in messages:
        try:
            event = json.loads(message["Body"])
        except (ValueError, KeyError, TypeError):
            done.append(message["ReceiptHandle"])
            continue
        payload = alarm_page(config, event, opened)
        if payload is None or f"{payload['incidentId']}:{payload['action']}" in sent:
            done.append(message["ReceiptHandle"])
            continue
        received = now()
        try:
            status = send(payload)
        except OSError as exc:  # left on the queue: delivered again after its visibility timeout
            print(f"relay: webhook failed: {exc}", file=sys.stderr, flush=True)
            continue
        record = {
            "incident_id": payload["incidentId"],
            "action": payload["action"],
            "alarm_name": event["detail"].get("alarmName"),
            "alarm_arn": event["resources"][0],
            "transition": payload["timestamp"],
            "event_id": event.get("id"),
            "event_time": event.get("time"),
            "relayed_at": datetime.fromtimestamp(received, timezone.utc).isoformat(timespec="milliseconds"),
            "status": status,
        }
        out.write(json.dumps(record, sort_keys=True) + "\n")
        out.flush()
        if status == 200:
            sent.add(f"{payload['incidentId']}:{payload['action']}")
            if payload["action"] == "created":
                opened[event["resources"][0]] = payload["incidentId"]
            else:
                opened.pop(event["resources"][0], None)
            done.append(message["ReceiptHandle"])
    return done


def relay(config: Config, hook: Webhook, ledger: Path, aws: Callable[..., dict[str, Any]]) -> None:
    """Run until killed: long-poll the queue and relay each transition (at least once; the incident id dedups)."""
    url = aws("sqs", "get-queue-url", "--queue-name", config.queue_name)["QueueUrl"]
    sent, opened = ledger_state(ledger)
    with ledger.open("a") as out:
        while True:
            try:
                got = aws(
                    "sqs",
                    "receive-message",
                    "--queue-url",
                    url,
                    "--wait-time-seconds",
                    "20",
                    "--max-number-of-messages",
                    "10",
                    "--visibility-timeout",
                    "30",
                )
            except RuntimeError as exc:
                print(f"relay: {exc}", file=sys.stderr, flush=True)
                time.sleep(5)
                continue
            for handle in relay_once(
                config, got.get("Messages", []), lambda p: post(hook, p), sent, out, opened=opened
            ):
                try:
                    aws("sqs", "delete-message", "--queue-url", url, "--receipt-handle", handle)
                except RuntimeError as exc:
                    print(f"relay: {exc}", file=sys.stderr, flush=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config", type=Path, help="JSON: account, region, lane, journeys, default_policy, policies")
    parser.add_argument("action", choices=["render", "apply", "teardown", "publish", "relay"])
    parser.add_argument("--outcomes", type=Path, help="publish: the FUZZOPS_JOURNEY_OUTCOMES file")
    parser.add_argument("--webhook", type=Path, help="relay: the associate-service output with the HMAC webhook")
    parser.add_argument("--ledger", type=Path, help="relay: JSON lines of relayed transitions")
    args = parser.parse_args(argv)
    config = Config.load(json.loads(args.config.read_text()))
    aws = aws_cli(config.region)
    if args.action == "render":
        print(json.dumps(render(config), indent=2, sort_keys=True))
    elif args.action == "apply":
        print(json.dumps(apply(config, aws), indent=2, sort_keys=True))
    elif args.action == "teardown":
        print("\n".join(teardown(config, aws)))
    elif args.action == "publish":
        if args.outcomes is None:
            parser.error("publish needs --outcomes")
        publish(config, args.outcomes, aws)
    else:
        if args.webhook is None or args.ledger is None:
            parser.error("relay needs --webhook and --ledger")
        relay(config, Webhook.load(args.webhook), args.ledger, aws)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
