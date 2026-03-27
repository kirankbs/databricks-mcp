from datetime import datetime, timezone
from typing import Any


def format_duration(milliseconds: int | None) -> str:
    if milliseconds is None:
        return "—"
    seconds = milliseconds / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours = int(minutes // 60)
    mins = int(minutes % 60)
    return f"{hours}h {mins}m"


def format_bytes(byte_count: int | None) -> str:
    if byte_count is None:
        return "—"
    n = float(byte_count)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


def ms_to_str(ms: int | None) -> str:
    if ms is None:
        return "—"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def truncate_stacktrace(text: str | None, max_lines: int = 30) -> str:
    if not text:
        return ""
    lines = text.strip().splitlines()
    if len(lines) <= max_lines:
        return text.strip()
    head = 5
    tail = max_lines - head
    omitted = len(lines) - max_lines
    return "\n".join(lines[:head]) + f"\n... ({omitted} lines omitted) ...\n" + "\n".join(lines[-tail:])


def enum_val(obj: Any, fallback: str = "?") -> str:
    """Safely extract string value from a SDK enum or return fallback."""
    if obj is None:
        return fallback
    return obj.value if hasattr(obj, "value") else str(obj)


def deduplicate_events(events: list[dict[str, Any]], type_key: str = "type") -> list[dict[str, Any]]:
    """Collapse runs of identical event types into summary entries."""
    if not events:
        return events
    result: list[dict[str, Any]] = []
    run_start = 0
    for i in range(1, len(events) + 1):
        at_end = i == len(events)
        same_type = not at_end and events[i][type_key] == events[run_start][type_key]
        if not same_type:
            count = i - run_start
            if count == 1:
                result.append(events[run_start])
            else:
                summary = dict(events[run_start])
                summary["_repeated"] = count
                summary["_first_timestamp"] = events[run_start].get("timestamp")
                summary["_last_timestamp"] = events[i - 1].get("timestamp")
                result.append(summary)
            run_start = i
    return result
