from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from crocus_raw.model import InfluxPoint


@dataclass(frozen=True)
class InstrumentRule:
    instrument_id: str
    match: Mapping[str, str]


@dataclass(frozen=True)
class InstrumentIdentity:
    instrument_id: str
    identity_source: str
    confidence: str
    review_required: bool
    node: str
    kind: str
    zone: str


class InstrumentResolver:
    def __init__(self, rules: list[InstrumentRule] | None = None, require_registry: bool = False):
        self.rules = rules or []
        self.require_registry = require_registry
        canonical_rules = [
            {"instrument_id": rule.instrument_id, "match": dict(sorted(rule.match.items()))}
            for rule in self.rules
        ]
        payload = json.dumps(canonical_rules, separators=(",", ":"), sort_keys=True)
        self.fingerprint = hashlib.sha256(payload.encode()).hexdigest()

    @classmethod
    def from_json(cls, path: Path, require_registry: bool = False) -> InstrumentResolver:
        document = json.loads(path.read_text())
        if set(document) != {"rules"} or not isinstance(document["rules"], list):
            raise ValueError("instrument registry must contain only a rules list")
        rules: list[InstrumentRule] = []
        seen_ids: set[str] = set()
        for index, item in enumerate(document["rules"]):
            if not isinstance(item, dict) or set(item) != {"instrument_id", "match"}:
                raise ValueError(f"registry rule {index} must contain instrument_id and match")
            instrument_id = item["instrument_id"]
            match = item["match"]
            if not isinstance(instrument_id, str) or not instrument_id:
                raise ValueError(f"registry rule {index} has invalid instrument_id")
            if instrument_id in seen_ids:
                raise ValueError(f"duplicate instrument_id {instrument_id!r}")
            if not isinstance(match, dict) or not match or not all(
                isinstance(key, str) and isinstance(value, str) for key, value in match.items()
            ):
                raise ValueError(f"registry rule {index} has invalid match")
            seen_ids.add(instrument_id)
            rules.append(InstrumentRule(instrument_id, match))
        return cls(rules, require_registry=require_registry)

    def resolve(self, point: InfluxPoint) -> str:
        return self.resolve_identity(point).instrument_id

    def resolve_identity(self, point: InfluxPoint) -> InstrumentIdentity:
        for rule in self.rules:
            if all(point.tags.get(key) == value for key, value in rule.match.items()):
                node, _ = _first_tag(point.tags, ("node", "vsn", "host"), "unknown-node")
                kind, _ = _instrument_kind(point)
                zone = point.tags.get("zone") or "unknown-zone"
                return InstrumentIdentity(rule.instrument_id, "registry", "high", False, node, kind, zone)
        if self.require_registry:
            raise ValueError(
                f"no instrument registry rule matched measurement {point.measurement!r} tags {dict(point.tags)!r}"
            )
        return _fallback_instrument_identity(point)


def _fallback_instrument_identity(point: InfluxPoint) -> InstrumentIdentity:
    node, node_source = _first_tag(point.tags, ("node", "vsn", "host"), "unknown-node")
    kind, kind_source = _instrument_kind(point)
    zone = point.tags.get("zone") or "unknown-zone"
    identity = {"node": node, "kind": kind, "zone": zone}
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:12]
    readable = "--".join(_slug(value) for value in (node, kind, zone))
    confidence = "high" if kind_source == "sensor" and node_source == "node" else "medium"
    if kind_source in {"system", "measurement"} or node_source in {"host", "default"}:
        confidence = "low"
    return InstrumentIdentity(
        instrument_id=f"{readable[:120]}--{digest}",
        identity_source=f"{node_source}+{kind_source}",
        confidence=confidence,
        review_required=confidence != "high",
        node=node,
        kind=kind,
        zone=zone,
    )


def _instrument_kind(point: InfluxPoint) -> tuple[str, str]:
    for key in ("sensor", "task", "deviceName", "device"):
        if value := point.tags.get(key):
            return value, key
    if point.measurement.startswith("sys."):
        return "system", "system"
    return point.measurement.split(".", 1)[0], "measurement"


def _first_tag(
    tags: Mapping[str, str], keys: tuple[str, ...], default: str
) -> tuple[str, str]:
    for key in keys:
        if value := tags.get(key):
            return value, key
    return default, "default"


def _slug(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-.").lower()
    return normalized or "unknown"
