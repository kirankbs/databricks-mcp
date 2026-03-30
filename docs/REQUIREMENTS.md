# Databricks Debug MCP Server — Implementation Plan

## Context

A staff data/ML engineer investigates Spark job failures and performance issues on Azure Databricks by manually navigating 6+ browser tabs: job run history → task error output → driver logs (stdout/stderr/JVM heap) → cluster metrics (CPU/memory/swap) → cluster event log (DRIVER_NOT_RESPONDING, METASTORE_DOWN) → Spark UI stages (44K stages, shuffle I/O, task counts). This MCP server eliminates the copy-paste friction by exposing all this data programmatically to Claude.

**What exists**: Several Databricks MCP servers (Unity Catalog, code execution, Genie) exist but none cover the debugging/investigation workflow. Kubeflow has a Spark History Server MCP but it's standalone. This is a genuine gap.

**What we learned from API research**:
- Jobs API 2.1 fully supports run listing, details, and task output retrieval
- Clusters API fully supports config retrieval and event log queries
- **There is NO direct REST API to retrieve driver stdout/stderr** — logs must be read from DBFS via `cluster_log_conf` path
- **Azure Log Analytics does NOT contain driver stdout/stderr** — only audit/operational events. Not useful for log content retrieval.
- Spark UI proxy (`/driver-proxy-api/o/0/{cluster_id}/40001/api/v1/...`) only works when cluster is RUNNING — terminated job clusters cannot serve Spark UI data
- Structured Streaming has no dedicated REST API — monitoring is via Spark UI streaming tab (same proxy limitation) or StreamingQueryListener (requires code on cluster)
- DBFS read API has 1MB-per-call limit

---

## Architecture

**Stack**: Python MCP server using `mcp` SDK (FastMCP) + `databricks-sdk` for all Databricks API access + `httpx` for Spark UI proxy calls.

**Auth**: Databricks profile from `~/.databrickscfg` via `DATABRICKS_CONFIG_PROFILE` env var (user already has profiles configured with `auth_type: databricks-cli`).

**Packaging**: `src/` layout with hatchling (local-only `pip install -e .`), consistent with user's conventions.

### Project Structure

```
databricks-mcp/
├── pyproject.toml
├── .env.example
├── src/
│   └── databricks_debug_mcp/
│       ├── __init__.py
│       ├── server.py              # FastMCP entry point, tool registration
│       ├── client.py              # WorkspaceClient singleton + Spark UI proxy helper
│       ├── config.py              # Env-based config dataclass
│       ├── tools/
│       │   ├── __init__.py
│       │   ├── jobs.py            # Job runs, task output, error extraction
│       │   ├── clusters.py        # Cluster config, events, state
│       │   ├── logs.py            # Driver/executor logs via DBFS
│       │   ├── spark_ui.py        # Spark UI data via driver-proxy-api (live clusters only)
│       │   └── diagnostics.py     # Cross-run comparison, timeline, one-shot investigation
│       └── formatting.py          # Structured output for LLM consumption
```

### Dependencies

```toml
[project]
dependencies = [
    "mcp>=1.0.0",
    "databricks-sdk>=0.40.0",
    "httpx>=0.27.0",
]
```

No Azure SDK dependencies — Log Analytics doesn't contain the data we need (driver logs), and the Databricks SDK + Clusters Events API covers operational events better.

---

## Tools — 17 total across 4 phases

### Phase 1 — Core Investigation (MVP)

These 7 tools cover the exact 6-tab workflow from the screenshots.

**1. `list_job_runs`**
- List recent runs for a job. Filter by status, time range. Returns run_id, status, duration, trigger type.
- Params: `job_name: str | None`, `job_id: int | None`, `limit: int = 10`, `status_filter: str | None` (SUCCESS/FAILED/TIMED_OUT/CANCELLED)
- API: `w.jobs.list()` to resolve name → id, then `w.jobs.list_runs(job_id=..., expand_tasks=True)` with page_token pagination
- Output: Table of runs with columns: run_id, start_time, duration, status, trigger, task_summary

**2. `get_run_details`**
- Deep dive on a single run — all task statuses, durations, error messages, cluster assignments.
- Params: `run_id: int`
- API: `w.jobs.get_run(run_id=run_id)`
- Output: Run metadata + per-task breakdown: task_key, state (life_cycle + result), duration, cluster_id, attempt_number

**3. `get_task_output`**
- Error output and stack trace for a specific task within a multi-task job run. Auto-selects failed tasks if task_key not provided.
- Params: `run_id: int`, `task_key: str | None`
- API: `w.jobs.get_run(run_id)` to get task run IDs, then `w.jobs.get_run_output(run_id=task_run_id)` per task
- Output: error_message, truncated stack trace (parsed for key exception class + message), notebook_output if any, logs field

**4. `get_cluster_info`**
- Cluster config and current state. Returns node types, Spark version, spark_conf, autoscaling, log delivery path, termination reason.
- Params: `cluster_id: str`
- API: `w.clusters.get(cluster_id=cluster_id)`
- Output: Structured config dump — spark_version, node_type_id, driver_node_type_id, num_workers/autoscale, spark_conf dict, cluster_log_conf (if set), state, termination_reason

**5. `get_cluster_events`**
- Cluster event log entries: DRIVER_NOT_RESPONDING, METASTORE_DOWN, NODES_LOST, autoscaling, init script failures, etc.
- Params: `cluster_id: str`, `event_types: list[str] | None`, `hours_back: float = 24`, `limit: int = 50`
- API: `w.clusters.events(cluster_id=..., start_time=epoch_ms, end_time=now_ms, order="DESC")` with page_token pagination (max 500 per page)
- Output: Chronological event list with timestamp, type, details. Auto-deduplicates repeating patterns (e.g., alternating DRIVER_NOT_RESPONDING/METASTORE_DOWN → summarized as "repeated 15 times between HH:MM and HH:MM").

**6. `get_driver_logs`**
- Read driver stdout/stderr from DBFS cluster log delivery path. Supports tailing and pattern search.
- Params: `cluster_id: str`, `log_type: str = "stderr"` (stdout | stderr), `tail_lines: int = 200`, `search_pattern: str | None`
- Implementation:
  1. `w.clusters.get(cluster_id)` → extract `cluster_log_conf.dbfs.destination` or `cluster_log_conf.s3/azure/gcs` path
  2. Construct path: `{destination}/{cluster_id}/driver/{log_type}`
  3. `w.dbfs.get_status(path)` → get file_size
  4. Calculate offset for tail: `offset = max(0, file_size - (tail_lines * 200))` (estimate 200 bytes/line)
  5. `w.dbfs.read(path=path, offset=offset, length=1048576)` → decode base64
  6. If `search_pattern` provided, filter matching lines with 2 lines of context
- Fallback: If `cluster_log_conf` is not set, return error explaining that log delivery must be configured and how to enable it.
- Output: Log content (last N lines or matching lines), plus auto-detected patterns: OOM markers, DriverStoppedException, GC pressure indicators, heap dumps

**7. `get_cluster_for_run`**
- Resolve which cluster(s) a job run used. Handles ephemeral job clusters and shared existing clusters.
- Params: `run_id: int`
- API: `w.jobs.get_run(run_id)` → extract `cluster_instance.cluster_id` from each task's `cluster_instance`
- Output: Dict mapping task_key → {cluster_id, cluster_name, is_job_cluster}

### Phase 2 — Spark UI Integration (live clusters only)

These 4 tools access the Spark driver's REST API via the Databricks proxy. **Only works when the cluster is in RUNNING state.** Each tool gracefully reports when the cluster is terminated and suggests alternatives.

**Spark UI proxy URL**: `https://{workspace_host}/driver-proxy-api/o/0/{cluster_id}/40001/api/v1/applications/{app_id}/...`

Auth: Same Databricks token as Bearer header. App ID auto-discovered via `/applications` endpoint.

**8. `get_spark_stages`**
- Stage details: task counts, duration, shuffle read/write, input/output bytes.
- Params: `cluster_id: str`, `sort_by: str = "duration"`, `limit: int = 20`, `status: str | None` (ACTIVE/COMPLETE/FAILED)
- API: Spark History Server `/stages` endpoint via proxy
- Output: Top-N stages sorted by chosen metric. For each: stage_id, description (truncated), duration, numTasks (succeeded/total), inputBytes, outputBytes, shuffleReadBytes, shuffleWriteBytes

**9. `get_spark_executors`**
- Executor resource stats: memory, disk, tasks, failures, shuffle totals.
- Params: `cluster_id: str`
- API: Spark History Server `/allexecutors` endpoint via proxy
- Output: Per-executor table: id, host, totalCores, maxMemory, memoryUsed, diskUsed, activeTasks, completedTasks, failedTasks, totalShuffleRead, totalShuffleWrite. Plus summary row.

**10. `get_spark_sql_queries`**
- SQL/DataFrame query executions with physical plans and metrics.
- Params: `cluster_id: str`, `query_id: int | None`, `limit: int = 10`
- API: Spark History Server `/sql` endpoint via proxy
- Output: Per-query: id, description (first 200 chars), duration, runningJobIds, successJobIds, failedJobIds. If `query_id` specified, includes full physical plan.

**11. `get_spark_jobs`**
- Spark-level job list (not Databricks jobs — these are Spark actions like count(), save(), etc.)
- Params: `cluster_id: str`, `status: str | None` (RUNNING/SUCCEEDED/FAILED), `limit: int = 20`
- API: Spark History Server `/jobs` endpoint via proxy
- Output: Per-job: jobId, name, numStages, numTasks, numCompletedTasks, numFailedTasks, submissionTime, completionTime, duration

### Phase 3 — Streaming Metrics (live clusters only)

**12. `get_streaming_queries`**
- List streaming queries with status and latest progress summary.
- Params: `cluster_id: str`
- API: Spark UI proxy — `/api/v1/applications/{app_id}/streaming/statistics` (if available), otherwise parse from SQL queries endpoint where description contains "streaming"
- Output: Per-query: id, name, isActive, recentProgress (last batch duration, input rows/s, processed rows/s, watermark)
- Fallback: If endpoint not available, suggest using StreamingQueryListener in application code for persistent metrics.

**13. `get_streaming_query_progress`**
- Detailed progress for a specific streaming query: batch-level metrics.
- Params: `cluster_id: str`, `query_id: str`
- API: Same as above, filtered to specific query
- Output: Recent batches with: batchId, batchDuration, numInputRows, inputRowsPerSecond, processedRowsPerSecond, stateOperators (numRowsTotal, memoryUsedBytes), sources (startOffset, endOffset, latestOffset)

### Phase 4 — Diagnostics & Correlation (compound tools)

These orchestrate Phase 1-3 tools to answer higher-level questions.

**14. `compare_runs`**
- Side-by-side comparison of 2-5 job runs.
- Params: `run_ids: list[int]`
- Output: Per-task timing comparison (duration delta, % change), status changes across runs, cluster config differences, new/resolved errors

**15. `get_run_timeline`**
- Unified chronological timeline for a single run — merges task lifecycle events with cluster events.
- Params: `run_id: int`
- Output: Chronologically ordered events: task started, task completed/failed, DRIVER_NOT_RESPONDING, METASTORE_DOWN, cluster termination, etc. Each with timestamp and context.

**16. `investigate_failure`**
- One-shot failure investigation. Gathers everything relevant, applies heuristic classification.
- Params: `run_id: int`
- Output: Structured diagnostic: failure_category (OOM, driver_crash, metastore, timeout, infra, unknown), root_cause_hypothesis, evidence (error messages, cluster events, log excerpts if available), affected_tasks, suggested_next_steps

**17. `search_logs`**
- Search driver logs across stdout and stderr for a regex pattern with context.
- Params: `cluster_id: str`, `pattern: str`, `context_lines: int = 2`
- Output: Matching lines grouped by log type, with context lines before/after

---

## Key Technical Decisions

### Log Access
- **Primary path**: DBFS via `cluster_log_conf` → `{destination}/{cluster_id}/driver/{log_type}`
- **DBFS 1MB limit**: Use `get_status()` for file size, calculate offset for tail reads
- **No Log Analytics**: Research confirmed Azure Log Analytics contains audit events only, NOT driver stdout/stderr. Not worth the extra dependency.
- **Fallback**: If `cluster_log_conf` not set, clear error message with instructions to enable it. Also return whatever `get_run_output` provides (task-level logs field, which is limited but better than nothing).

### Spark UI Proxy Limitations
- **Only works on RUNNING clusters** — terminated job clusters cannot serve data
- **URL**: `https://{host}/driver-proxy-api/o/0/{cluster_id}/40001/api/v1/...`
- **Graceful degradation**: Each Spark UI tool checks cluster state first. If terminated, returns: "Cluster {id} is terminated. Spark UI data unavailable. Available alternatives: get_run_details (task timings), get_driver_logs (log content), get_cluster_events (infrastructure events)."
- **Future improvement**: Parse Spark event logs from DBFS (`spark.eventLog.dir`) for terminated clusters — same data the History Server would display. Deferred to a future phase.

### LLM-Friendly Output Formatting
- **Stages**: Top-N slowest by default, not all 44K. Include sort_by parameter.
- **Logs**: Last N lines with auto-detected error patterns highlighted (prefixed with `>>> `)
- **Events**: Deduplicate repeating patterns into summaries
- **Comparisons**: Tabular format with delta columns

### Streaming Metrics Approach
- The Spark UI streaming tab data may be accessible via the same proxy API
- If the `/streaming/statistics` endpoint doesn't exist on the proxy, fall back to parsing SQL queries with streaming-related descriptions
- For terminated clusters, streaming metrics are not available — suggest implementing `StreamingQueryListener` that writes progress to a Delta table for historical access

---

## Implementation Order

### Step 1: Project skeleton
- `pyproject.toml` with hatchling build, entry point `databricks-debug-mcp`
- `server.py` — FastMCP instance, tool registration
- `client.py` — `get_workspace_client()` singleton using profile auth, `spark_ui_request()` helper for proxy calls
- `config.py` — Config dataclass from env vars
- `formatting.py` — `format_duration()`, `format_bytes()`, `truncate_stacktrace()`
- Verify: `pip install -e .` then `databricks-debug-mcp` starts and registers tools

### Step 2: Phase 1 tools — jobs.py
- `list_job_runs`, `get_run_details`, `get_task_output`
- Test against a known failed job

### Step 3: Phase 1 tools — clusters.py
- `get_cluster_info`, `get_cluster_events`, `get_cluster_for_run`
- Test against cluster from the failed run

### Step 4: Phase 1 tools — logs.py
- `get_driver_logs`, `search_logs`
- Test against a cluster with log delivery configured

### Step 5: Phase 2 tools — spark_ui.py (Spark UI section)
- `get_spark_stages`, `get_spark_executors`, `get_spark_sql_queries`, `get_spark_jobs`
- Requires a running cluster to test the proxy URL

### Step 6: Phase 3 tools — spark_ui.py (streaming section)
- `get_streaming_queries`, `get_streaming_query_progress`
- Requires a running cluster with active streaming queries

### Step 7: Phase 4 tools — diagnostics.py
- `compare_runs`, `get_run_timeline`, `investigate_failure`
- Test `investigate_failure` end-to-end on a known failed run

### Step 8: Wire up MCP config
- Create `.mcp.json` for Claude Code integration
- Test full investigation workflow: "investigate the failed feature-extraction job from last week"

---

## Critical Files

| File | Purpose |
|------|---------|
| `pyproject.toml` | Package definition, entry point, dependencies |
| `src/databricks_debug_mcp/server.py` | FastMCP entry point, all tool registration |
| `src/databricks_debug_mcp/client.py` | WorkspaceClient + Spark UI proxy HTTP helper |
| `src/databricks_debug_mcp/config.py` | Environment-based configuration |
| `src/databricks_debug_mcp/tools/jobs.py` | Tools 1-3: list_job_runs, get_run_details, get_task_output |
| `src/databricks_debug_mcp/tools/clusters.py` | Tools 4-5, 7: cluster info, events, run→cluster resolution |
| `src/databricks_debug_mcp/tools/logs.py` | Tools 6, 17: driver log reading and searching via DBFS |
| `src/databricks_debug_mcp/tools/spark_ui.py` | Tools 8-13: Spark UI data + streaming metrics via proxy |
| `src/databricks_debug_mcp/tools/diagnostics.py` | Tools 14-16: compare_runs, timeline, investigate_failure |
| `src/databricks_debug_mcp/formatting.py` | Duration/bytes formatting, stacktrace truncation, dedup |

## Verification

1. `pip install -e .` — package installs cleanly
2. `databricks-debug-mcp` — server starts, lists all 17 tools
3. Add to Claude Code `.mcp.json` — tools appear in Claude's tool list
4. Test Phase 1: `list_job_runs(job_name="my-etl-job")` → `get_run_details(run_id=...)` → `get_task_output(run_id=...)` → `get_cluster_events(cluster_id=...)` → `get_driver_logs(cluster_id=...)`
5. Test Phase 2: Run against a live cluster — verify Spark UI proxy URL works
6. Test Phase 4: `investigate_failure(run_id=...)` returns structured diagnostic
7. Full integration: Ask Claude "investigate the failed ETL job" — verify Claude uses the tools in the right sequence

## Decisions Made

- **Auth**: Databricks profile via `~/.databrickscfg` + `DATABRICKS_CONFIG_PROFILE` env var
- **Distribution**: Local-only (`pip install -e .`)
- **No Azure Log Analytics**: Confirmed it doesn't contain driver logs, only audit events
- **Spark UI**: Live clusters only via driver-proxy-api. Graceful degradation for terminated clusters.
- **Streaming**: Via Spark UI proxy where possible; suggest StreamingQueryListener for historical data
- **Scope**: 17 tools across 4 phases
