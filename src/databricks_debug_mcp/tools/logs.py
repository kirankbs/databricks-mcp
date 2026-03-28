import base64
import re

from mcp.server.fastmcp import FastMCP

from ..client import get_workspace_client
from ..formatting import format_bytes

# Patterns that warrant highlighting (prefixed with >>> in output)
_ERROR_PATTERNS = re.compile(
    r"OutOfMemoryError|GC overhead limit|DriverStoppedException"
    r"|FATAL|heap dump|killed.*SIGKILL|java\.lang\.RuntimeException"
    r"|Container killed|ExecutorLostFailure|Lost executor",
    re.IGNORECASE,
)


def register(mcp: FastMCP) -> None:

    @mcp.tool()
    def get_driver_logs(
        cluster_id: str,
        log_type: str = "stderr",
        tail_lines: int = 200,
        search_pattern: str | None = None,
    ) -> str:
        """Read driver stdout or stderr from DBFS log delivery path.

        Requires cluster_log_conf to be configured on the cluster.
        Returns last tail_lines lines, or lines matching search_pattern with 2 lines of context.
        Error patterns (OOM, GC, DriverStoppedException) are prefixed with >>> for visibility.
        """
        w = get_workspace_client()

        try:
            cluster = w.clusters.get(cluster_id=cluster_id)
        except Exception as e:
            return f"Failed to get cluster {cluster_id}: {e}"

        log_conf = cluster.cluster_log_conf
        if not log_conf:
            return (
                f"Log delivery is not configured for cluster {cluster_id}.\n"
                "Enable it by setting cluster_log_conf.dbfs.destination in the cluster config.\n"
                "Example: dbfs:/cluster-logs\n\n"
                "Note: get_task_output may still have limited task-level logs via the Jobs API."
            )

        if log_conf.dbfs:
            base_path = log_conf.dbfs.destination.rstrip("/")
        else:
            return (
                f"Cluster {cluster_id} uses non-DBFS log delivery. "
                "Only DBFS (dbfs:/) log delivery is currently supported."
            )

        log_path = f"{base_path}/{cluster_id}/driver/{log_type}"

        try:
            file_info = w.dbfs.get_status(path=log_path)
        except Exception as e:
            return (
                f"Log file not found at {log_path}: {e}\n"
                "Logs are written asynchronously — wait a few minutes after job completion and retry."
            )

        file_size = file_info.file_size or 0
        # Estimate 200 bytes/line; cap at DBFS 1 MB limit per read
        offset = max(0, file_size - tail_lines * 200)
        read_length = min(1_048_576, file_size - offset)

        try:
            read_resp = w.dbfs.read(path=log_path, offset=offset, length=read_length)
        except Exception as e:
            return f"Failed to read {log_path}: {e}"

        raw = base64.b64decode(read_resp.data or "").decode("utf-8", errors="replace")
        lines = raw.splitlines()
        if len(lines) > tail_lines:
            lines = lines[-tail_lines:]

        if search_pattern:
            _, result = _search_in_lines(lines, search_pattern, log_type, log_path)
            return result

        highlighted = []
        for line in lines:
            prefix = ">>> " if _ERROR_PATTERNS.search(line) else "    "
            highlighted.append(prefix + line)

        header = (
            f"Driver {log_type} — cluster {cluster_id}\n"
            f"File: {log_path} ({format_bytes(file_size)} total, showing last {len(lines)} lines)\n"
        )
        return header + "\n".join(highlighted)

    @mcp.tool()
    def search_logs(
        cluster_id: str,
        pattern: str,
        context_lines: int = 2,
    ) -> str:
        """Search driver stdout and stderr for a regex pattern with context lines.

        Searches across both log types and returns all matches. Useful for finding
        specific exception classes, task IDs, or custom log messages.
        """
        w = get_workspace_client()

        try:
            cluster = w.clusters.get(cluster_id=cluster_id)
        except Exception as e:
            return f"Failed to get cluster {cluster_id}: {e}"

        log_conf = cluster.cluster_log_conf
        if not log_conf or not log_conf.dbfs:
            return (
                f"DBFS log delivery is not configured for cluster {cluster_id}. "
                "Cannot search logs without cluster_log_conf.dbfs set."
            )

        base_path = log_conf.dbfs.destination.rstrip("/")
        sections = []

        for log_type in ("stderr", "stdout"):
            log_path = f"{base_path}/{cluster_id}/driver/{log_type}"
            try:
                file_info = w.dbfs.get_status(path=log_path)
                file_size = file_info.file_size or 0
                offset = max(0, file_size - 500 * 200)
                read_resp = w.dbfs.read(path=log_path, offset=offset, length=min(1_048_576, file_size - offset))
                raw = base64.b64decode(read_resp.data or "").decode("utf-8", errors="replace")
                lines = raw.splitlines()[-500:]
            except Exception:
                continue

            found, result = _search_in_lines(lines, pattern, log_type, log_path, context=context_lines)
            if found:
                sections.append(result)

        if not sections:
            return f"Pattern '{pattern}' not found in driver logs for cluster {cluster_id}."

        return "\n\n".join(sections)


def _search_in_lines(lines: list[str], pattern: str, log_type: str, log_path: str, context: int = 2) -> tuple[bool, str]:
    """Search lines for pattern. Returns (found, result_text)."""
    if len(pattern) > 200:
        return False, "Pattern too long (max 200 chars)."
    try:
        rx = re.compile(pattern, re.IGNORECASE)
    except re.error as e:
        return False, f"Invalid regex pattern '{pattern}': {e}"

    match_indices = [i for i, line in enumerate(lines) if rx.search(line)]
    if not match_indices:
        return False, f"Pattern '{pattern}' not found in {log_type} (searched {len(lines)} lines of {log_path})."

    include: set[int] = set()
    for idx in match_indices:
        for j in range(max(0, idx - context), min(len(lines), idx + context + 1)):
            include.add(j)

    output_lines = []
    prev = -1
    match_set = set(match_indices)
    for idx in sorted(include):
        if prev >= 0 and idx - prev > 1:
            output_lines.append("...")
        prefix = ">>> " if idx in match_set else "    "
        output_lines.append(f"{prefix}{lines[idx]}")
        prev = idx

    header = f"Pattern '{pattern}' in {log_type} — {len(match_indices)} match(es):\nFile: {log_path}\n"
    return True, header + "\n".join(output_lines)
