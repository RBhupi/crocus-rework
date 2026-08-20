from __future__ import annotations

import fnmatch
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from crocus_raw.model import InfluxPoint


SELECTION_VERSION = 2
SUPPORTED_SELECTION_VERSIONS = frozenset({1, 2})
_GLOB_CHARACTERS = frozenset("*?[")


@dataclass(frozen=True)
class Selector:
    measurement: str | None = None
    measurement_glob: str | None = None
    fields: tuple[str, ...] | None = None
    tags: tuple[tuple[str, tuple[str, ...]], ...] = ()

    def matches(self, point: InfluxPoint) -> bool:
        return self.matches_parts(point.measurement, point.field, point.tags)

    def matches_parts(self, measurement: str, field: str, tags: Mapping[str, str]) -> bool:
        if self.measurement is not None and measurement != self.measurement:
            return False
        if self.measurement_glob is not None and not fnmatch.fnmatchcase(
            measurement, self.measurement_glob
        ):
            return False
        if self.fields is not None and not _matches_any(field, self.fields):
            return False
        for key, patterns in self.tags:
            value = tags.get(key)
            if value is None or not _matches_any(value, patterns):
                return False
        return True

    def matches_measurement_field(self, measurement: str, field: str) -> bool:
        if self.measurement is not None and measurement != self.measurement:
            return False
        if self.measurement_glob is not None and not fnmatch.fnmatchcase(
            measurement, self.measurement_glob
        ):
            return False
        return self.fields is None or _matches_any(field, self.fields)

    def document(self) -> dict[str, object]:
        document: dict[str, object] = {}
        if self.measurement is not None:
            document["measurement"] = self.measurement
        if self.measurement_glob is not None:
            document["measurement_glob"] = self.measurement_glob
        if self.fields is not None:
            document["fields"] = list(self.fields)
        if self.tags:
            document["tags"] = {key: list(values) for key, values in self.tags}
        return document


@dataclass(frozen=True)
class Selection:
    version: int
    selectors: tuple[Selector, ...]
    canonical_json: str
    fingerprint: str

    @classmethod
    def from_json(cls, path: Path) -> Selection:
        try:
            document = json.loads(path.read_text())
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid selection JSON: {error}") from error
        return cls.from_document(document)

    @classmethod
    def from_document(cls, document: object) -> Selection:
        if not isinstance(document, dict) or set(document) != {"selection_version", "selectors"}:
            raise ValueError("selection must contain only selection_version and selectors")
        version = document["selection_version"]
        if version not in SUPPORTED_SELECTION_VERSIONS:
            raise ValueError(
                f"selection_version must be one of {sorted(SUPPORTED_SELECTION_VERSIONS)}"
            )
        raw_selectors = document["selectors"]
        if not isinstance(raw_selectors, list) or not raw_selectors:
            raise ValueError("selectors must be a non-empty list")
        selectors = tuple(
            _parse_selector(item, index, version) for index, item in enumerate(raw_selectors)
        )
        canonical_documents = sorted(
            {json.dumps(selector.document(), separators=(",", ":"), sort_keys=True) for selector in selectors}
        )
        canonical_document = {
            "selection_version": version,
            "selectors": [json.loads(item) for item in canonical_documents],
        }
        canonical_json = json.dumps(canonical_document, separators=(",", ":"), sort_keys=True)
        return cls(
            version=version,
            selectors=tuple(
                _parse_selector(item, index, version)
                for index, item in enumerate(canonical_document["selectors"])
            ),
            canonical_json=canonical_json,
            fingerprint=hashlib.sha256(canonical_json.encode()).hexdigest(),
        )

    @classmethod
    def from_measurements(cls, measurements: set[str] | tuple[str, ...]) -> Selection:
        return cls.from_document(
            {
                "selection_version": 1,
                "selectors": [{"measurement": measurement} for measurement in sorted(set(measurements))],
            }
        )

    @property
    def measurements(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    selector.measurement
                    for selector in self.selectors
                    if selector.measurement is not None
                }
            )
        )

    @property
    def requires_discovery(self) -> bool:
        return any(selector.measurement_glob is not None for selector in self.selectors)

    def matches(self, point: InfluxPoint) -> bool:
        return any(selector.matches(point) for selector in self.selectors)

    def matches_parts(self, measurement: str, field: str, tags: Mapping[str, str]) -> bool:
        return any(
            selector.matches_parts(measurement, field, tags) for selector in self.selectors
        )

    def matches_measurement_field(self, measurement: str, field: str) -> bool:
        return any(
            selector.matches_measurement_field(measurement, field)
            for selector in self.selectors
        )

    def write(self, path: Path) -> None:
        path.write_text(json.dumps(json.loads(self.canonical_json), indent=2, sort_keys=True) + "\n")


def _parse_selector(document: object, index: int, version: int) -> Selector:
    if not isinstance(document, dict):
        raise ValueError(f"selector {index} must be an object")
    allowed_keys = {"measurement", "fields", "tags"}
    if version >= 2:
        allowed_keys.add("measurement_glob")
    unknown_keys = set(document) - allowed_keys
    if unknown_keys:
        raise ValueError(f"selector {index} has unknown keys: {sorted(unknown_keys)}")
    measurement = document.get("measurement")
    measurement_glob = document.get("measurement_glob")
    if (measurement is None) == (measurement_glob is None):
        raise ValueError(
            f"selector {index} must contain exactly one of measurement or measurement_glob"
        )
    if measurement is not None and (not isinstance(measurement, str) or not measurement):
        raise ValueError(f"selector {index} measurement must be a non-empty string")
    if measurement is not None and any(character in measurement for character in _GLOB_CHARACTERS):
        raise ValueError(f"selector {index} measurement must be exact, not a glob")
    if measurement_glob is not None and (
        version < 2 or not isinstance(measurement_glob, str) or not measurement_glob
    ):
        raise ValueError(f"selector {index} measurement_glob must be a non-empty string")

    raw_fields = document.get("fields")
    fields = None if raw_fields is None else _parse_patterns(raw_fields, f"selector {index} fields")

    raw_tags = document.get("tags", {})
    if not isinstance(raw_tags, dict):
        raise ValueError(f"selector {index} tags must be an object")
    if not all(isinstance(key, str) and key for key in raw_tags):
        raise ValueError(f"selector {index} tag keys must be non-empty strings")
    if "node" in raw_tags:
        raise ValueError(
            f"selector {index} cannot use discarded tag 'node'; select by 'vsn' instead"
        )
    tags: list[tuple[str, tuple[str, ...]]] = []
    for key in sorted(raw_tags):
        tags.append((key, _parse_patterns(raw_tags[key], f"selector {index} tag {key!r}")))
    return Selector(
        measurement=measurement,
        measurement_glob=measurement_glob,
        fields=fields,
        tags=tuple(tags),
    )


def _parse_patterns(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{label} must be a non-empty list")
    if not all(isinstance(item, str) and item for item in value):
        raise ValueError(f"{label} values must be non-empty strings")
    return tuple(sorted(set(value)))


def _matches_any(value: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatchcase(value, pattern) for pattern in patterns)
