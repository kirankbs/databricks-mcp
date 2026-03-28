---
name: databricks-debug
description: Investigate Databricks Spark job failures, cluster issues, performance regressions, Delta table health, cost attribution, audit forensics, table lineage, DLT pipeline errors, and init script issues using the databricks-debug MCP tools. Triggers on: job failure, cluster crash, OOM, Spark debugging, run investigation, driver logs, slow job, streaming lag, Delta small files, table health, job cost, query performance, skew, spill, audit trail, lineage, blast radius, DLT pipeline, init script.
---

# Databricks Debug Skill

Use this skill whenever the user asks to investigate a Databricks job failure, cluster issue, slow run, streaming problem, Delta table health, cost question, security audit, table lineage, DLT pipeline error, or init script failure. The `databricks-debug` MCP server provides 30 raw data access tools -- this skill teaches you how to orchestrate them.

---

## Standard Investigation Flow

Always follow this sequence for a job failure. Skip steps where data isn't available.

### Step 1 — Find the failing run
```
list_job_runs(job_name="<name>", status_filter="FAILED", limit=5)
```

### Step 2 — Get task breakdown
```
get_run_details(run_id=<run_id>)
```
Note which tasks failed, their durations, and cluster IDs.

### Step 3 — Get the stack trace
```
get_task_output(run_id=<run_id>)
```
Auto-selects failed tasks. For a specific task: `get_task_output(run_id=<id>, task_key="<key>")`

### Step 4 — Resolve cluster IDs
```
get_cluster_for_run(run_id=<run_id>)
```

### Step 5 — Check cluster events
```
get_cluster_events(cluster_id=<cluster_id>)
```
Look for: `DRIVER_NOT_RESPONDING`, `METASTORE_DOWN`, `NODES_LOST`, spot preemption.

### Step 6 — Read driver logs
```
get_driver_logs(cluster_id=<cluster_id>, log_type="stderr", tail_lines=200)
search_logs(cluster_id=<cluster_id>, pattern="<exception_class>")
```

### Step 7 — Check cluster config
```
get_cluster_info(cluster_id=<cluster_id>)
```

---

## Performance Investigation Flow

For slow jobs, use the enhanced Spark analysis tools.

### Step 1 — Identify slow stages and detect skew
```
analyze_stage_skew(cluster_id=<cluster_id>)
```
This is the single most valuable performance tool. It compares max vs median task duration per stage, flags data skew (max > 3x median), surfaces spill to memory/disk, and identifies shuffle hotspots.

### Step 2 — Check Spark configuration
```
get_spark_config(cluster_id=<cluster_id>)
```
Auto-detects anti-patterns: AQE disabled, low shuffle partitions, aggressive broadcast threshold, missing dynamic allocation. Categorizes all settings by Memory/Shuffle/SQL/Executor/Driver.

### Step 3 — Check library status (for ClassNotFoundException)
```
get_library_status(cluster_id=<cluster_id>)
```
Shows INSTALLED/FAILED/PENDING status for every library. Surfaces failure messages.

### Step 4 — Cross-run cost comparison
```
get_job_cost(job_id=<id>, days_back=30)
```
Shows DBU cost per run, identifies cost anomalies.

---

## Delta Table Health Investigation

For Delta performance issues, small file problems, or table maintenance auditing.

### Full table health check
```
get_table_health(table_name="catalog.schema.table")
```
Returns: file count, avg file size, partition layout, last OPTIMIZE/VACUUM times, auto-optimize settings. Flags: small files (<32MB avg), high file count (>10k), missing auto-optimize, stale maintenance.

### Transaction history
```
get_table_history(table_name="catalog.schema.table", limit=30)
get_table_history(table_name="catalog.schema.table", operation_filter="MERGE")
```
Shows every write/merge/optimize/vacuum/schema change with metrics. Auto-detects schema evolution events.

### Predictive optimization audit
```
get_predictive_optimization(table_name="catalog.schema.table", days_back=7)
get_predictive_optimization(schema_name="production", days_back=30)
```
Shows what Databricks auto-optimization actually did: COMPACTION, VACUUM, ZORDER runs with DBU costs.

---

## Cost Investigation

### Job cost attribution
```
get_job_cost(run_id=<run_id>)
get_job_cost(job_name="etl-pipeline", days_back=30)
```
Joins system.billing.usage with list_prices for dollar amounts. Shows DBU consumption per run, per SKU.

### Expensive query analysis
```
get_query_history(duration_ms_gt=60000, hours_back=24)
get_query_history(user="expensive_user@company.com")
get_query_history(error_only=True)
```
Pulls from system.query.history: statement text, duration, rows produced, bytes scanned, errors.

### Cluster utilization / right-sizing
```
get_cluster_utilization(cluster_id=<id>, hours_back=24)
```
Node-level timeline from system.compute: when workers joined/left, peak concurrent count, total worker-hours. Identifies over-provisioned clusters.

---

## Security & Audit Forensics

### Who changed what?
```
get_audit_events(user="admin@company.com", hours_back=48)
get_audit_events(action="delete", service="clusters")
get_audit_events(service="unityCatalog", hours_back=168)
```
Pulls from system.access.audit: every API call, permission change, login, secret access.

---

## Failure Classification

Read the evidence and reason about the cause. Don't guess — classify only when you have clear signals.

### OOM (Out of Memory)
**Signals**: `OutOfMemoryError`, `GC overhead limit exceeded`, `Container killed`, `heap dump` in driver logs. `ExecutorLostFailure` in task output.
**Next steps**: Check `get_cluster_info` for memory settings. Use `analyze_stage_skew` to find spill. Check `get_spark_config` for memory overhead settings.

### Data Skew
**Signals**: `analyze_stage_skew` shows skew ratio > 3x. One executor doing 90% of work in `get_spark_executors`.
**Next steps**: Identify the skewed key. Suggest salting, repartition, or broadcast join hint.

### Driver Crash
**Signals**: `DRIVER_NOT_RESPONDING` in cluster events. `DriverStoppedException` in logs. SIGKILL.
**Next steps**: Check for driver OOM. Check for large `collect()` calls.

### Metastore / HMS Issue
**Signals**: `METASTORE_DOWN` event. `HiveMetaStoreClient` errors, `TTransportException` in logs.
**Next steps**: Usually transient. Check if it hit multiple runs at the same time.

### Node Loss / Spot Preemption
**Signals**: `NODES_LOST`, `NODE_BLACKLISTED` events. `ExecutorLostFailure` in logs.
**Next steps**: Enable spot fallback or switch to on-demand for critical runs.

### Small File Problem
**Signals**: `get_table_health` shows avg file size < 32MB or file count > 10k.
**Next steps**: Run OPTIMIZE. Enable auto-optimize. Check write patterns (too many small appends).

---

## Spark UI (Live Clusters Only)

- Slow stages: `get_spark_stages(cluster_id=..., sort_by="duration")`
- Skew/spill: `analyze_stage_skew(cluster_id=...)`
- Executor stats: `get_spark_executors(cluster_id=...)`
- SQL plans: `get_spark_sql_queries(cluster_id=..., query_id=<id>)`
- Full config: `get_spark_config(cluster_id=...)`
- Libraries: `get_library_status(cluster_id=...)`
- Spark jobs: `get_spark_jobs(cluster_id=...)`

For terminated clusters: `get_driver_logs`, `get_task_output`, `get_cluster_events`.

---

## System Tables (Requires SQL Warehouse)

These tools query Databricks system tables via the SQL Statement Execution API. They need `DATABRICKS_WAREHOUSE_ID` set.

- Cost: `get_job_cost` — system.billing.usage + list_prices
- Queries: `get_query_history` — system.query.history
- Utilization: `get_cluster_utilization` — system.compute.node_timeline
- Audit: `get_audit_events` — system.access.audit
- Delta health: `get_table_health` / `get_table_history` — DESCRIBE DETAIL/HISTORY
- Auto-maintenance: `get_predictive_optimization` — system.storage

---

## Table Lineage / Blast Radius

### Upstream: what feeds this table?
```
get_table_lineage(table_name="catalog.schema.table", direction="upstream")
```

### Downstream: what breaks if this table fails?
```
get_table_lineage(table_name="catalog.schema.table", direction="downstream")
```

### Column-level: where does this column come from?
```
get_column_lineage(table_name="catalog.schema.table", column_name="feature_col")
```

---

## DLT Pipeline Investigation

### Pipeline status and recent updates
```
get_pipeline_status(pipeline_name="my-etl-pipeline")
get_pipeline_status(pipeline_id="p-abc-123")
```

### Error events with stack traces
```
get_pipeline_errors(pipeline_id="p-abc-123")
```

### Data quality expectation results
```
get_pipeline_data_quality(pipeline_id="p-abc-123")
```

---

## Init Script Debugging

### Full init script audit
```
get_init_script_logs(cluster_id=<cluster_id>)
```
Shows: configured scripts, cluster events for init script execution (exit codes), and log file contents from DBFS. Auto-highlights errors (permission denied, not found, fail).

---

## Quick Reference

| Question | Tools |
|----------|-------|
| What failed and when? | `list_job_runs` → `get_run_details` |
| What's the stack trace? | `get_task_output` |
| What cluster was used? | `get_cluster_for_run` |
| Did infra fail? | `get_cluster_events` |
| What do the driver logs say? | `get_driver_logs` / `search_logs` |
| What's the cluster config? | `get_cluster_info` |
| Is there data skew? | `analyze_stage_skew` |
| Which stage is slow? (live only) | `get_spark_stages` |
| Is memory the problem? (live only) | `get_spark_executors` |
| What's the query plan? (live only) | `get_spark_sql_queries` |
| What Spark settings are applied? | `get_spark_config` |
| Are libraries installed correctly? | `get_library_status` |
| How much did this job cost? | `get_job_cost` |
| What queries are slow? | `get_query_history` |
| Is this cluster over-provisioned? | `get_cluster_utilization` |
| Who changed what? | `get_audit_events` |
| Is this Delta table healthy? | `get_table_health` |
| What operations hit this table? | `get_table_history` |
| What did auto-optimize do? | `get_predictive_optimization` |
| What feeds this table? | `get_table_lineage` (upstream) |
| What breaks if this table fails? | `get_table_lineage` (downstream) |
| Where does this column come from? | `get_column_lineage` |
| Why did the DLT pipeline fail? | `get_pipeline_errors` |
| What's the DLT pipeline state? | `get_pipeline_status` |
| Are DLT expectations passing? | `get_pipeline_data_quality` |
| Did init scripts fail? | `get_init_script_logs` |
