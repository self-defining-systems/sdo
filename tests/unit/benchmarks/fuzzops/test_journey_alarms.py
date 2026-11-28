from __future__ import annotations

import base64
import hashlib
import hmac
import io
import json
from typing import TYPE_CHECKING

import pytest

from benchmarks.fuzzops.journey_alarms import (
    Aggregator,
    AlarmPolicy,
    Config,
    Tail,
    Webhook,
    alarm_page,
    parse_time,
    relay_once,
    render,
)

if TYPE_CHECKING:
    from pathlib import Path

ACCOUNT = "000000000000"


def config(**policies: dict) -> Config:
    return Config.load(
        {
            "account": ACCOUNT,
            "region": "us-west-2",
            "lane": "z9",
            "journeys": ["read-thing", "write-thing", "write-thing-readback"],
            "default_policy": {"period_seconds": 10, "threshold": 0.2, "evaluation_periods": 3, "datapoints": 2},
            "policies": policies,
        }
    )


def outcome(at: str, journey: str, ok: bool, ms: int = 50) -> str:
    return json.dumps({"at": at, "journey": journey, "ok": ok, "latency_ms": ms})


def alarm_event(cfg: Config, journey: str, state: str = "ALARM", at: str = "2026-01-01T00:00:10.123+0000") -> dict:
    return {
        "id": "e-1",
        "source": "aws.cloudwatch",
        "detail-type": "CloudWatch Alarm State Change",
        "account": ACCOUNT,
        "region": "us-west-2",
        "time": "2026-01-01T00:00:10Z",
        "resources": [cfg.alarm_arn(journey)],
        "detail": {
            "alarmName": cfg.alarm_name(journey),
            "state": {"value": state, "reason": "free text", "timestamp": at},
        },
    }


def test_one_alarm_per_journey_on_its_failure_ratio_and_a_rule_for_exactly_those_alarms() -> None:
    cfg = config(**{"write-thing": {"threshold": 0.5}})
    out = render(cfg)
    names = [a["AlarmName"] for a in out["alarms"]]
    assert names == ["z9-journey-read-thing", "z9-journey-write-thing", "z9-journey-write-thing-readback"]
    write = out["alarms"][1]
    assert write["Threshold"] == 0.5
    assert write["DatapointsToAlarm"] == 2
    assert write["EvaluationPeriods"] == 3
    ratio = [m for m in write["Metrics"] if m["ReturnData"]]
    assert ratio == [{**ratio[0], "Expression": "IF(requests > 0, failed / requests)"}]
    dims = write["Metrics"][0]["MetricStat"]["Metric"]["Dimensions"]
    assert dims == [{"Name": "Lane", "Value": "z9"}, {"Name": "Journey", "Value": "write-thing"}]
    pattern = out["rule"]["EventPattern"]
    assert pattern["resources"] == [cfg.alarm_arn(j) for j in cfg.journeys]
    assert pattern["detail"] == {"state": {"value": ["ALARM", "OK"]}}


def test_config_refuses_bad_policies_and_names() -> None:
    with pytest.raises(ValueError, match="threshold"):
        AlarmPolicy(threshold=0)
    with pytest.raises(ValueError, match="datapoints"):
        AlarmPolicy(datapoints=4, evaluation_periods=3)
    with pytest.raises(ValueError, match="period_seconds"):
        AlarmPolicy(period_seconds=45)
    with pytest.raises(ValueError, match="unknown journeys"):
        config(unknown={"threshold": 0.5})
    with pytest.raises(ValueError, match="invalid lane"):
        Config.load({"account": ACCOUNT, "region": "us-west-2", "lane": "Z 9", "journeys": ["a"]})


def test_closed_periods_count_every_journey_and_skip_unknown_or_malformed_lines() -> None:
    cfg = config()
    agg = Aggregator(cfg)
    for line in (
        outcome("2026-01-01T00:00:01.123456789Z", "read-thing", True),
        outcome("2026-01-01T00:00:02Z", "read-thing", False, 3000),
        outcome("2026-01-01T00:00:03Z", "write-thing-readback", False),
        outcome("2026-01-01T00:00:12Z", "read-thing", True),
        outcome("2026-01-01T00:00:04Z", "not-a-journey", False),
        "not json",
    ):
        agg.add(line)
    start = parse_time("2026-01-01T00:00:00Z").timestamp()
    assert agg.closed(start + 14) == []  # the first period is still open for late finishers
    data = agg.closed(start + 15)
    counts = {(d["Dimensions"][1]["Value"], d["MetricName"]): d.get("Value") for d in data}
    assert counts[("read-thing", "Requests")] == 2
    assert counts[("read-thing", "Failed")] == 1
    assert counts[("write-thing", "Requests")] == 0
    assert ("write-thing", "Latency") not in counts
    assert counts[("write-thing-readback", "Failed")] == 1
    latency = next(d for d in data if d["MetricName"] == "Latency" and d["Dimensions"][1]["Value"] == "read-thing")
    assert latency["StatisticValues"]["Maximum"] == 3000
    assert {d["Timestamp"] for d in data} == {"2026-01-01T00:00:00+00:00"}
    assert agg.closed(start + 15) == []  # a period is published once
    assert len(agg.closed(start + 30)) == 2 * 3 + 1  # the 00:00:10 period


def test_a_lane_alarm_entering_alarm_pages_once_with_trusted_fields_only() -> None:
    cfg = config()
    event = alarm_event(cfg, "write-thing")
    page = alarm_page(cfg, event)
    assert page is not None
    expected = hashlib.sha256(f"{cfg.alarm_arn('write-thing')}\n2026-01-01T00:00:10.123+0000".encode()).hexdigest()
    assert page["incidentId"] == expected
    assert page["timestamp"] == "2026-01-01T00:00:10.123+0000"
    assert "free text" not in json.dumps(page)
    assert alarm_page(cfg, alarm_event(cfg, "write-thing", state="OK")) is None
    foreign = {**event, "resources": ["arn:aws:cloudwatch:us-west-2:000000000000:alarm:other"]}
    assert alarm_page(cfg, foreign) is None
    assert alarm_page(cfg, {**event, "account": "111111111111"}) is None


def test_return_to_ok_resolves_the_incident_the_alarm_opened() -> None:
    cfg = config()
    alarm = alarm_event(cfg, "write-thing", at="2026-01-01T00:00:10.123+0000")
    ok = alarm_event(cfg, "write-thing", state="OK", at="2026-01-01T00:05:00.000+0000")
    created = alarm_page(cfg, alarm)
    assert created is not None
    assert created["action"] == "created"
    # no ledger: the id comes from the previous ALARM state the OK event reports
    assert alarm_page(cfg, ok) is None
    ok["detail"]["previousState"] = {"value": "ALARM", "timestamp": "2026-01-01T00:00:10.123+0000"}
    resolved = alarm_page(cfg, ok)
    assert resolved is not None
    assert (resolved["action"], resolved["incidentId"]) == ("resolved", created["incidentId"])
    assert resolved["timestamp"] == "2026-01-01T00:05:00.000+0000"
    # the relay's own record of what is open wins
    arn = cfg.alarm_arn("write-thing")
    assert alarm_page(cfg, ok, {arn: "open-id"})["incidentId"] == "open-id"  # type: ignore[index]


def test_relay_resolves_an_episode_before_the_next_one_opens_a_new_incident() -> None:
    cfg = config()
    msgs = [
        {"Body": json.dumps(alarm_event(cfg, "read-thing", at="2026-01-01T00:00:10.000+0000")), "ReceiptHandle": "a"},
        {
            "Body": json.dumps(alarm_event(cfg, "read-thing", state="OK", at="2026-01-01T00:02:00.000+0000")),
            "ReceiptHandle": "b",
        },
        {"Body": json.dumps(alarm_event(cfg, "read-thing", at="2026-01-01T00:20:00.000+0000")), "ReceiptHandle": "c"},
        {
            "Body": json.dumps(alarm_event(cfg, "read-thing", state="OK", at="2026-01-01T00:02:00.000+0000")),
            "ReceiptHandle": "d",
        },
    ]
    sent: list[dict] = []
    ledger = io.StringIO()
    keys: set[str] = set()
    opened: dict[str, str] = {}
    done = relay_once(cfg, msgs, lambda p: sent.append(dict(p)) or 200, keys, ledger, lambda: 0.0, opened)
    assert done == ["a", "b", "c", "d"]
    assert [p["action"] for p in sent] == ["created", "resolved", "created", "resolved"]
    assert sent[0]["incidentId"] == sent[1]["incidentId"] != sent[2]["incidentId"] == sent[3]["incidentId"]
    assert opened == {}
    assert [json.loads(line)["action"] for line in ledger.getvalue().splitlines()] == [
        "created",
        "resolved",
        "created",
        "resolved",
    ]


def test_relay_sends_each_transition_once_and_keeps_failed_sends_on_the_queue() -> None:
    cfg = config()
    first = {"Body": json.dumps(alarm_event(cfg, "read-thing")), "ReceiptHandle": "h1"}
    replay = {"Body": json.dumps(alarm_event(cfg, "read-thing")), "ReceiptHandle": "h2"}
    later = {
        "Body": json.dumps(alarm_event(cfg, "read-thing", at="2026-01-01T00:05:00.000+0000")),
        "ReceiptHandle": "h3",
    }
    junk = {"Body": "{", "ReceiptHandle": "h4"}
    sent: list[dict] = []
    ledger = io.StringIO()
    ids: set[str] = set()
    done = relay_once(
        cfg, [first, replay, later, junk], lambda p: sent.append(dict(p)) or 200, ids, ledger, lambda: 0.0
    )
    assert done == ["h1", "h2", "h3", "h4"]
    assert len(sent) == 2
    records = [json.loads(line) for line in ledger.getvalue().splitlines()]
    assert [r["alarm_name"] for r in records] == ["z9-journey-read-thing"] * 2
    assert {r["incident_id"] for r in records} == {p["incidentId"] for p in sent} == {k.split(":")[0] for k in ids}

    def down(_: object) -> int:
        raise OSError("unreachable")

    retry = {"Body": json.dumps(alarm_event(cfg, "write-thing")), "ReceiptHandle": "h5"}
    assert relay_once(cfg, [retry], down, ids, io.StringIO()) == []


def test_webhook_signs_timestamp_and_body_with_the_hmac_secret() -> None:
    hook = Webhook("https://event-ai.us-west-2.api.aws/webhook/generic/abc_DEF-1", "s3cret")
    headers = hook.headers(b'{"a":1}', "2026-01-01T00:00:00.000Z")
    want = base64.b64encode(hmac.digest(b"s3cret", b'2026-01-01T00:00:00.000Z:{"a":1}', "sha256")).decode()
    assert headers["x-amzn-event-signature"] == want
    assert "s3cret" not in repr(hook)
    with pytest.raises(ValueError, match="webhook URL"):
        Webhook("https://example.com/webhook/generic/abc", "s")


def test_tail_reads_appended_lines_across_truncation(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.jsonl"
    tail = Tail(path)
    assert tail.read() == []
    path.write_text('{"a":1}\n{"b"')
    assert tail.read() == ['{"a":1}']
    with path.open("a") as f:
        f.write(":2}\n")
    assert tail.read() == ['{"b":2}']
    path.write_text('{"c":3}\n')
    assert tail.read() == ['{"c":3}']
