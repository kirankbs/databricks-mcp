---
name: databricks-debug
description: Investigate Databricks Spark job failures, cluster issues, and performance regressions using the databricks-debug MCP tools. Triggers on: job failure, cluster crash, OOM, Spark debugging, run investigation, driver logs, slow job, streaming lag.
---

# Databricks Debug Skill

Use this skill whenever the user asks to investigate a Databricks job failure, cluster issue, slow run, or streaming problem. The `databricks-debug` MCP server provides 14 raw data access tools — this skill teaches you how to orchestrate them.

---

## Standard Investigation Flow

Always follow this sequence for a job failure. Skip steps where data isn't available (e.g., no run_id yet).

### Step 1 — Find the failing run
If you have a job name but no run_id:
```
list_job_runs(job_name="<name>", status_filter="FAILED", limit=5)
```
If you have a job_id:
```
list_job_runs(job_id=<id>, status_filter="FAILED", limit=5)
```

### Step 2 — Get task breakdown
```
get_run_details(run_id=<run_id>)
```
Note which tasks failed, their durations, and cluster IDs. Each task may have its own cluster.

### Step 3 — Get the stack trace
```
get_task_output(run_id=<run_id>)
```
If multiple tasks failed, it auto-selects them. For a specific task:
```
get_task_output(run_id=<run_id>, task_key="<key>")
```
Read the full stack trace — the actual exception class and message is the most important signal.

### Step 4 — Resolve cluster IDs
```
get_cluster_for_run(run_id=<run_id>)
```
Returns a mapping of task_key → cluster_id. Job clusters are ephemeral; this resolves them even after termination.

### Step 5 — Check cluster events
```
get_cluster_events(cluster_id=<cluster_id>)
```
Look for: `DRIVER_NOT_RESPONDING`, `METASTORE_DOWN`, `NODES_LOST`, `NODE_BLACKLISTED`, spot preemption events.
These are often the real cause — the task error is a symptom.

### Step 6 — Read driver logs (if log delivery configured)
```
get_driver_logs(cluster_id=<cluster_id>, log_type="stderr", tail_lines=200)
```
Lines prefixed with `>>>` are auto-highlighted error patterns. If you know what to look for:
```
get_driver_logs(cluster_id=<cluster_id>, search_pattern="OutOfMemoryError")
```
Or search both stdout/stderr at once:
```
search_logs(cluster_id=<cluster_id>, pattern="<exception_class>")
```

### Step 7 — Check cluster config (if needed)
```
get_cluster_info(cluster_id=<cluster_id>)
```
Check: executor memory, driver memory, spark_conf settings, termination reason.

---

## Failure Classification

Read the evidence and reason about the cause. Don't guess — classify only when you have clear signals.

### OOM (Out of Memory)
**Signals**: `OutOfMemoryError`, `GC overhead limit exceeded`, `Container killed on request`, `heap dump` in driver logs. Task result state `FAILED` with `ExecutorLostFailure`.
**Next steps**: Check `get_cluster_info` for executor/driver memory settings. Suggest increasing `spark.executor.memory`, repartitioning, or caching less aggressively.

### Driver Crash
**Signals**: `DRIVER_NOT_RESPONDING` in cluster events (especially repeated). `DriverStoppedException` in logs. SIGKILL in logs.
**Next steps**: Check if the driver itself is OOM (driver-side `OutOfMemoryError`). Check if the Spark UI proxy was connected to a live cluster — might reveal a large `collect()` or broadcast join.

### Metastore / HMS Issue
**Signals**: `METASTORE_DOWN` cluster event. `Unable to instantiate SparkContext`, `HiveMetaStoreClient` errors, `TTransportException` in logs.
**Next steps**: Usually transient. Check if it affected multiple runs at the same time (suggests infra issue, not code).

### Node Loss / Spot Preemption
**Signals**: `NODES_LOST`, `NODE_BLACKLISTED` cluster events. `ExecutorLostFailure`, `Lost executor` in logs.
**Next steps**: If spot/preemptible workers, this is expected under high demand. Suggest enabling spot fallback or switching to on-demand for critical runs. Increase `spark.task.maxFailures`.

### Timeout
**Signals**: Task result state `TIMED_OUT`. No exception in task output.
**Next steps**: Check task durations in `get_run_details`. Which task hit the timeout? Was it stalled or genuinely slow? Check cluster events for any node issues during that period.

### Application Error
**Signals**: Stack trace in `get_task_output` with a clear exception class (e.g., `AnalysisException`, `NullPointerException`, `FileNotFoundException`).
**Next steps**: Read the full exception message. This is a code/data issue, not infrastructure. The stack trace points to the exact line.

---

## Cross-Run Comparison

When the user asks "why is this run slower/different than a previous run?":

1. Call `get_run_details` for both run IDs
2. Build a side-by-side task duration table manually from the results
3. Identify which specific tasks regressed
4. For the regressed task, compare clusters: `get_cluster_info` for each run's cluster — different node types? different spark_conf?
5. If the cluster is still running, `get_spark_stages(cluster_id=..., sort_by="duration")` to find the slowest stage

---

## Timeline Construction

When the user asks "walk me through what happened":

1. `get_run_details` — note task start/end timestamps
2. `get_cluster_events(cluster_id=..., hours_back=<duration of run + buffer>)` — get infra events for the same window
3. Merge them into a chronological narrative: task starts, cluster events, task failures

---

## Spark UI (Live Clusters Only)

These tools only work when the cluster is **RUNNING**. For a completed/terminated job cluster, they return an error with alternatives.

- Slow stages: `get_spark_stages(cluster_id=..., sort_by="duration")`
- Executor skew: `get_spark_executors(cluster_id=...)` — compare task counts and shuffle across executors
- Slow SQL plan: `get_spark_sql_queries(cluster_id=..., query_id=<id>)` — includes physical plan
- Active Spark jobs: `get_spark_jobs(cluster_id=...)`

For terminated clusters, fall back to: `get_driver_logs`, `get_task_output`, `get_cluster_events`.

---

## Streaming Investigations

For streaming jobs on running clusters:
```
get_streaming_queries(cluster_id=<cluster_id>)
get_streaming_query_progress(cluster_id=<cluster_id>, query_id=<id>)
```
For terminated streaming clusters: suggest implementing a `StreamingQueryListener` that writes progress events to a Delta table. No historical streaming metrics survive cluster termination.

---

## Quick Reference

| Question | Tools |
|----------|-------|
| What failed and when? | `list_job_runs` → `get_run_details` |
| What's the stack trace? | `get_task_output` |
| What cluster was used? | `get_cluster_for_run` |
| Did infra fail? | `get_cluster_events` |
| What does the driver log say? | `get_driver_logs` / `search_logs` |
| What's the cluster config? | `get_cluster_info` |
| Which stage is slow? (live only) | `get_spark_stages` |
| Is memory the problem? (live only) | `get_spark_executors` |
| What's the query plan? (live only) | `get_spark_sql_queries` |
