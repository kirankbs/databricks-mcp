"""Spark event log reader for post-mortem debugging of terminated clusters.

Reads event logs from cluster log delivery destinations (DBFS), decompresses
if needed, and parses Spark listener events for stage/task/executor analysis.
"""

from __future__ import annotations

import base64
import io
import json
import struct
from dataclasses import dataclass, field

from .client import get_workspace_client

_CHUNK_SIZE = 1_000_000  # DBFS read API max is 1MB


@dataclass
class EventLogConfig:
    cluster_id: str
    log_base_path: str
    files: list[str] = field(default_factory=list)


def discover_event_log_path(cluster_id: str) -> EventLogConfig:
    """Find where Spark event logs are stored for a cluster.

    Checks cluster_log_conf for the DBFS/S3 destination, then lists the
    eventlog subdirectory to find actual log files.
    """
    w = get_workspace_client()
    cluster = w.clusters.get(cluster_id=cluster_id)

    log_conf = cluster.cluster_log_conf
    if not log_conf:
        raise ValueError(
            f"Cluster {cluster_id} has no log delivery configured. "
            "Event logs are only available when cluster log delivery is enabled "
            "(Advanced Options > Logging in cluster config)."
        )

    if log_conf.dbfs:
        dest = log_conf.dbfs.destination.rstrip("/")
    elif log_conf.s3:
        dest = log_conf.s3.destination.rstrip("/")
    else:
        raise ValueError(
            f"Cluster {cluster_id} has log delivery configured but uses an "
            "unsupported destination type (only DBFS and S3 are supported)."
        )

    event_log_dir = f"{dest}/{cluster_id}/eventlog"

    files = []
    try:
        for entry in w.dbfs.list(event_log_dir):
            if entry.is_dir:
                try:
                    for sub_entry in w.dbfs.list(entry.path):
                        if not sub_entry.is_dir and sub_entry.file_size > 0:
                            files.append(sub_entry.path)
                except Exception:
                    pass
            elif entry.file_size and entry.file_size > 0:
                files.append(entry.path)
    except Exception as e:
        raise ValueError(
            f"Cannot list event logs at {event_log_dir}: {e}\n"
            "The cluster may not have written event logs yet, or the path structure differs."
        ) from e

    if not files:
        raise ValueError(f"No event log files found at {event_log_dir}")

    return EventLogConfig(
        cluster_id=cluster_id,
        log_base_path=event_log_dir,
        files=sorted(files),
    )


def read_dbfs_file(path: str, max_bytes: int = 200 * 1024 * 1024) -> bytes:
    """Read a complete file from DBFS, handling the 1MB chunk limit.

    Caps total read at max_bytes (default 200MB) to avoid OOM on huge event logs.
    Returns silently truncated data if the file exceeds max_bytes — callers
    cannot distinguish truncation from a complete read.
    """
    w = get_workspace_client()
    chunks = []
    offset = 0
    total = 0

    while True:
        resp = w.dbfs.read(path, offset=offset, length=_CHUNK_SIZE)
        if not resp.data:
            break

        chunk = base64.b64decode(resp.data)
        chunks.append(chunk)
        total += len(chunk)

        if total >= max_bytes:
            break
        bytes_read = resp.bytes_read or len(chunk)
        if bytes_read < _CHUNK_SIZE:
            break
        offset += bytes_read

    return b"".join(chunks)


def _detect_codec(path: str) -> str | None:
    """Detect compression codec from file extension."""
    lower = path.lower()
    if lower.endswith(".zstd") or lower.endswith(".zst"):
        return "zstd"
    if lower.endswith(".lz4"):
        return "lz4"
    if lower.endswith(".snappy"):
        return "snappy"
    return None


def _decompress_zstd(data: bytes) -> bytes:
    import zstandard as zstd

    return zstd.ZstdDecompressor().decompress(data, max_output_size=256 * 1024 * 1024)


_MAX_DECOMPRESS_SIZE = 256 * 1024 * 1024  # 256MB cap, matches zstd


def _decompress_hadoop_lz4(data: bytes) -> bytes:
    """Decompress Hadoop BlockCompressorStream LZ4 format.

    Spark uses Hadoop's block codec, not standard LZ4 frames. Each block has:
    4-byte BE original size, then sub-blocks of (4-byte BE compressed size, data).
    """
    import lz4.block

    result = bytearray()
    buf = io.BytesIO(data)

    while True:
        header = buf.read(4)
        if len(header) < 4:
            break
        original_size = struct.unpack(">I", header)[0]
        if original_size == 0:
            break

        remaining = original_size
        while remaining > 0:
            sub_header = buf.read(4)
            if len(sub_header) < 4:
                break
            compressed_size = struct.unpack(">I", sub_header)[0]
            compressed_data = buf.read(compressed_size)
            if len(compressed_data) < compressed_size:
                raise ValueError(f"Truncated LZ4 block: expected {compressed_size} bytes, got {len(compressed_data)}")
            decompressed = lz4.block.decompress(
                compressed_data,
                uncompressed_size=min(remaining, original_size),
            )
            result.extend(decompressed)
            remaining -= len(decompressed)
            if len(result) > _MAX_DECOMPRESS_SIZE:
                raise ValueError(f"LZ4 decompressed output too large (>{_MAX_DECOMPRESS_SIZE} bytes)")

    return bytes(result)


def _decompress_snappy(data: bytes) -> bytes:
    import snappy

    result = snappy.decompress(data)
    if len(result) > 256 * 1024 * 1024:
        raise ValueError(f"Snappy decompressed output too large ({len(result)} bytes, max 256MB)")
    return result


def decompress(data: bytes, codec: str | None) -> str:
    """Decompress event log data and return as UTF-8 text.

    LZ4 uses Hadoop BlockCompressorStream framing, not standard LZ4 frames —
    requires lz4.block, not lz4.frame.
    """
    if codec is None:
        return data.decode("utf-8")
    if codec == "zstd":
        return _decompress_zstd(data).decode("utf-8")
    if codec == "lz4":
        return _decompress_hadoop_lz4(data).decode("utf-8")
    if codec == "snappy":
        return _decompress_snappy(data).decode("utf-8")
    raise ValueError(f"Unsupported compression codec: {codec}")


def parse_events(text: str, event_types: set[str] | None = None) -> list[dict]:
    """Parse newline-delimited JSON event log, optionally filtering by event type.

    Skips malformed lines silently — event logs can have partial writes at EOF.
    """
    events = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        # Fast string pre-filter before full JSON parse — event type appears
        # near the start of each line, so this avoids parsing irrelevant events
        if event_types and not any(et in line for et in event_types):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue

        if event_types and event.get("Event") not in event_types:
            continue
        events.append(event)
    return events


def read_and_parse_event_log(
    cluster_id: str,
    event_types: set[str] | None = None,
    max_files: int = 5,
) -> tuple[list[dict], EventLogConfig]:
    """End-to-end: discover log path, read files, decompress, parse events.

    Returns (events, config) tuple. Reads the most recent files first (by name sort),
    capped at max_files to avoid reading gigabytes of old logs.
    """
    config = discover_event_log_path(cluster_id)

    files_to_read = config.files[-max_files:]

    all_events = []
    for file_path in files_to_read:
        raw = read_dbfs_file(file_path)
        codec = _detect_codec(file_path)

        try:
            text = decompress(raw, codec)
        except ImportError as e:
            module = str(e).split("'")[1] if "'" in str(e) else "unknown"
            raise ImportError(f"Compression codec requires additional package: pip install {module}") from e
        except Exception:
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                continue

        events = parse_events(text, event_types=event_types)
        all_events.extend(events)

    return all_events, config
