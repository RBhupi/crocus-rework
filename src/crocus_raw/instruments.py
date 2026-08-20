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
    vsn: str
    kind: str
    zone: str
    device: str


class InstrumentResolver:
    def __init__(self, rules: list[InstrumentRule] | None = None, require_registry: bool = False):
        self.rules = rules or []
        if any("node" in rule.match for rule in self.rules):
            raise ValueError("instrument registry cannot match discarded tag 'node'; use 'vsn'")
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
        return self.resolve_tags(point.measurement, point.tags)

    def resolve_tags(
        self, measurement: str, tags: Mapping[str, str]
    ) -> InstrumentIdentity:
        for rule in self.rules:
            if all(tags.get(key) == value for key, value in rule.match.items()):
                vsn = tags.get("vsn") or "unknown-vsn"
                kind, _ = _instrument_kind(measurement, tags)
                zone = tags.get("zone") or "unknown-zone"
                device = _device_identity(tags)
                return InstrumentIdentity(
                    rule.instrument_id,
                    "registry",
                    "high",
                    False,
                    vsn,
                    kind,
                    zone,
                    device,
                )
        if self.require_registry:
            raise ValueError(
                f"no instrument registry rule matched measurement {measurement!r} tags {dict(tags)!r}"
            )
        return _fallback_instrument_identity(measurement, tags)


def _fallback_instrument_identity(
    measurement: str, tags: Mapping[str, str]
) -> InstrumentIdentity:
    vsn = tags.get("vsn") or "unknown-vsn"
    kind, kind_source = _instrument_kind(measurement, tags)
    zone = tags.get("zone") or "unknown-zone"
    device = _device_identity(tags)
    identity = {"vsn": vsn, "kind": kind, "zone": zone, "device": device}
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:12]
    readable_parts = [_vsn_slug(vsn), _slug(kind), _slug(zone)]
    if device != "unknown-device":
        readable_parts.append(_slug(device))
    readable = "--".join(readable_parts)
    confidence = "high" if kind_source == "sensor" and vsn != "unknown-vsn" else "medium"
    if kind_source in {"system", "measurement"} or vsn == "unknown-vsn":
        confidence = "low"
    return InstrumentIdentity(
        instrument_id=f"{readable[:120]}--{digest}",
        identity_source=f"vsn+{kind_source}",
        confidence=confidence,
        review_required=confidence != "high",
        vsn=vsn,
        kind=kind,
        zone=zone,
        device=device,
    )


def _instrument_kind(
    measurement: str, tags: Mapping[str, str]
) -> tuple[str, str]:
    for key in ("sensor", "task", "deviceName", "device"):
        if value := tags.get(key):
            return value, key
    if measurement.startswith("sys."):
        return "system", "system"
    return measurement.split(".", 1)[0], "measurement"


def _device_identity(tags: Mapping[str, str]) -> str:
    return tags.get("deviceName") or tags.get("device") or "unknown-device"


def _slug(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-.").lower()
    return normalized or "unknown"


def _vsn_slug(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-.")
    return normalized or "unknown-vsn"
