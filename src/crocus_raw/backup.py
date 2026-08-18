from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from crocus_raw.runtime import sha256_file


@dataclass(frozen=True)
class BackupShard:
    shard_id: int
    archive: Path
    start_time: datetime
    end_time: datetime
    compressed_size: int


@dataclass(frozen=True)
class BackupBucket:
    bucket_id: str
    bucket_name: str
    manifest_path: Path
    manifest_sha256: str
    snapshot: str
    shards: tuple[BackupShard, ...]


def load_backup_bucket(backup_dir: Path, bucket_id: str) -> BackupBucket:
    manifests = sorted(
        path for path in backup_dir.glob("*.manifest") if not path.name.startswith("._")
    )
    if len(manifests) != 1:
        raise ValueError(f"expected exactly one backup manifest in {backup_dir}, found {len(manifests)}")
    manifest_path = manifests[0]
    document = json.loads(manifest_path.read_text())
    matching = [bucket for bucket in document.get("buckets", []) if bucket.get("bucketID") == bucket_id]
    if len(matching) != 1:
        raise ValueError(f"bucket {bucket_id!r} not found exactly once in {manifest_path}")
    bucket = matching[0]
    shards: list[BackupShard] = []
    for retention_policy in bucket.get("retentionPolicies", []):
        for shard_group in retention_policy.get("shardGroups", []):
            start_time = _parse_time(shard_group["startTime"])
            end_time = _parse_time(shard_group["endTime"])
            for shard in shard_group.get("shards", []):
                archive = backup_dir / shard["fileName"]
                if not archive.is_file():
                    raise FileNotFoundError(f"backup archive is missing: {archive}")
                shards.append(
                    BackupShard(
                        shard_id=int(shard["id"]),
                        archive=archive,
                        start_time=start_time,
                        end_time=end_time,
                        compressed_size=int(shard.get("size", archive.stat().st_size)),
                    )
                )
    snapshot = manifest_path.name.removesuffix(".manifest")
    return BackupBucket(
        bucket_id=bucket_id,
        bucket_name=bucket["bucketName"],
        manifest_path=manifest_path,
        manifest_sha256=sha256_file(manifest_path),
        snapshot=snapshot,
        shards=tuple(sorted(shards, key=lambda shard: (shard.start_time, shard.shard_id))),
    )


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))

