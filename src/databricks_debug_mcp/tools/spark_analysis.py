from mcp.server.fastmcp import FastMCP

from ..client import get_workspace_client, spark_ui_request, get_spark_app_id, assert_cluster_running
from ..formatting import format_duration, format_bytes, enum_val


def register(mcp: FastMCP) -> None:

    @mcp.tool()
    def analyze_stage_skew(
        cluster_id: str,
        stage_id: int | None = None,
        skew_threshold: float = 3.0,
    ) -> str:
        """Detect data skew and spill in Spark stages — the #1 cause of slow Spark jobs.

        Analyzes task-level metrics for each stage: compares max vs median task duration,
        flags spill to memory/disk, identifies straggler tasks, and surfaces shuffle hotspots.
        If stage_id is given, analyzes that stage in detail. Otherwise scans all completed stages.
        skew_threshold: flag stages where max_task_duration > threshold * median_task_duration.
        """
        err = assert_cluster_running(cluster_id)
        if err:
            return err

        try:
            app_id = get_spark_app_id(cluster_id)
            stages = spark_ui_request(cluster_id, f"applications/{app_id}/stages?status=complete")
        except Exception as e:
            return f"Failed to fetch stages: {e}"

        if stage_id is not None:
            stages = [s for s in stages if s.get("stageId") == stage_id]
            if not stages:
                return f"Stage {stage_id} not found."

        skewed_stages = []
        spill_stages = []
        all_analyses = []

        # Pre-filter: only fetch task summaries for stages worth analyzing
        # (enough tasks and non-trivial duration) to avoid N+1 API calls
        candidate_stages = [
            s for s in stages
            if s.get("numTasks", 0) >= 2 and s.get("executorRunTime", 0) > 1000
        ]
        # Cap at 30 stages to avoid excessive API calls
        candidate_stages = sorted(candidate_stages, key=lambda s: s.get("executorRunTime", 0), reverse=True)[:30]

        for stage in candidate_stages:
            sid = stage.get("stageId")
            attempt = stage.get("attemptId", 0)

            try:
                summary = spark_ui_request(
                    cluster_id,
                    f"applications/{app_id}/stages/{sid}/{attempt}/taskSummary?quantiles=0.0,0.25,0.5,0.75,0.9,0.95,1.0",
                )
            except Exception:
                continue

            analysis = _analyze_stage(sid, stage, summary, skew_threshold)
            all_analyses.append(analysis)

            if analysis["skew_ratio"] > skew_threshold:
                skewed_stages.append(analysis)
            if analysis["spill_memory"] > 0 or analysis["spill_disk"] > 0:
                spill_stages.append(analysis)

        if not all_analyses:
            return f"No completed stages with enough tasks to analyze on cluster {cluster_id}."

        lines = [f"Stage skew & spill analysis -- cluster {cluster_id} ({len(all_analyses)} stages analyzed):\n"]

        if skewed_stages:
            lines.append(f"SKEWED STAGES ({len(skewed_stages)}):")
            for a in sorted(skewed_stages, key=lambda x: x["skew_ratio"], reverse=True):
                lines.append(
                    f"  Stage {a['stage_id']}: skew ratio {a['skew_ratio']:.1f}x "
                    f"(median: {format_duration(a['median_ms'])}, max: {format_duration(a['max_ms'])}, "
                    f"tasks: {a['num_tasks']})"
                )
                lines.append(f"    Description: {a['description']}")
        else:
            lines.append("No skewed stages detected.")

        lines.append("")

        if spill_stages:
            lines.append(f"STAGES WITH SPILL ({len(spill_stages)}):")
            for a in sorted(spill_stages, key=lambda x: x["spill_disk"], reverse=True):
                lines.append(
                    f"  Stage {a['stage_id']}: "
                    f"spill memory={format_bytes(a['spill_memory'])}, "
                    f"spill disk={format_bytes(a['spill_disk'])}"
                )
        else:
            lines.append("No spill detected.")

        lines.append(f"\n{'Stage':<8} {'Tasks':<8} {'Median':<10} {'P95':<10} {'Max':<10} {'Skew':<8} {'Spill Mem':<12} {'Spill Disk':<12} {'Shuffle R':<12}")
        lines.append("-" * 100)

        top = sorted(all_analyses, key=lambda x: x["skew_ratio"], reverse=True)[:20]
        for a in top:
            lines.append(
                f"{a['stage_id']:<8} {a['num_tasks']:<8} "
                f"{format_duration(a['median_ms']):<10} {format_duration(a['p95_ms']):<10} "
                f"{format_duration(a['max_ms']):<10} {a['skew_ratio']:<8.1f} "
                f"{format_bytes(a['spill_memory']):<12} {format_bytes(a['spill_disk']):<12} "
                f"{format_bytes(a['shuffle_read']):<12}"
            )

        return "\n".join(lines)

    @mcp.tool()
    def get_spark_config(cluster_id: str) -> str:
        """Full Spark configuration as actually applied on a running cluster.

        Shows all spark.* settings, Hadoop settings, and system properties.
        Useful for debugging: "why is AQE disabled?" or "what's the shuffle partition count?"
        Flags known anti-patterns.
        """
        err = assert_cluster_running(cluster_id)
        if err:
            return err

        try:
            app_id = get_spark_app_id(cluster_id)
            env = spark_ui_request(cluster_id, f"applications/{app_id}/environment")
        except Exception as e:
            return f"Failed to fetch Spark environment: {e}"

        spark_props = env.get("sparkProperties", [])

        lines = [f"Spark configuration -- cluster {cluster_id}:\n"]

        categories = {
            "Memory": [], "Shuffle": [], "SQL/AQE": [],
            "Executor": [], "Driver": [], "IO/Compression": [], "Other": [],
        }

        for prop in spark_props:
            key = prop[0] if isinstance(prop, list) else prop.get("key", "?")
            val = prop[1] if isinstance(prop, list) else prop.get("value", "?")

            if any(x in key.lower() for x in ["memory", "overhead", "heap", "offheap"]):
                categories["Memory"].append((key, val))
            elif "shuffle" in key.lower():
                categories["Shuffle"].append((key, val))
            elif any(x in key.lower() for x in ["sql", "adaptive", "aqe", "broadcast", "join"]):
                categories["SQL/AQE"].append((key, val))
            elif "executor" in key.lower():
                categories["Executor"].append((key, val))
            elif "driver" in key.lower():
                categories["Driver"].append((key, val))
            elif any(x in key.lower() for x in ["io", "compress", "codec", "parquet", "orc"]):
                categories["IO/Compression"].append((key, val))
            else:
                categories["Other"].append((key, val))

        for cat, props in categories.items():
            if props:
                lines.append(f"\n[{cat}]")
                for k, v in sorted(props):
                    lines.append(f"  {k} = {v}")

        issues = _check_config_antipatterns(spark_props)
        if issues:
            lines.append(f"\n{'='*60}")
            lines.append("CONFIGURATION CONCERNS:")
            for issue in issues:
                lines.append(f"  [!] {issue}")

        return "\n".join(lines)

    @mcp.tool()
    def get_library_status(cluster_id: str) -> str:
        """Library installation status for a cluster — debug ClassNotFoundException and import errors.

        Shows all installed libraries, their status (INSTALLED, FAILED, PENDING, INSTALLING),
        and failure messages. Critical for debugging "ModuleNotFoundError" or "ClassNotFoundException".
        """
        w = get_workspace_client()

        try:
            statuses = w.libraries.cluster_status(cluster_id=cluster_id)
        except Exception as e:
            return f"Failed to get library status: {e}"

        if not statuses.library_statuses:
            return f"No libraries installed on cluster {cluster_id}."

        libs = statuses.library_statuses

        lines = [f"Libraries on cluster {cluster_id} ({len(libs)} total):\n"]
        lines.append(f"{'Library':<55} {'Status':<15} {'Messages'}")
        lines.append("-" * 100)

        failed = []
        for lib_status in libs:
            lib = lib_status.library
            status = enum_val(lib_status.status, fallback="?")

            lib_name = "?"
            if lib:
                if lib.pypi:
                    lib_name = f"pypi: {lib.pypi.package}"
                elif lib.maven:
                    lib_name = f"maven: {lib.maven.coordinates}"
                elif lib.jar:
                    lib_name = f"jar: {lib.jar}"
                elif lib.whl:
                    lib_name = f"whl: {lib.whl}"
                elif lib.egg:
                    lib_name = f"egg: {lib.egg}"

            lib_name = lib_name[:54]
            messages = " | ".join(lib_status.messages or [])[:50]
            lines.append(f"{lib_name:<55} {status:<15} {messages}")

            if status in ("FAILED", "SKIPPED"):
                failed.append((lib_name, lib_status.messages or []))

        if failed:
            lines.append(f"\nFAILED LIBRARIES ({len(failed)}):")
            for name, msgs in failed:
                lines.append(f"  {name}")
                for msg in msgs:
                    lines.append(f"    {msg}")

        return "\n".join(lines)


def _analyze_stage(stage_id: int, stage: dict, summary: dict, skew_threshold: float) -> dict:
    dur_quantiles = summary.get("executorRunTime", [0, 0, 0, 0, 0, 0, 0])
    median_ms = dur_quantiles[2] if len(dur_quantiles) > 2 else 0
    p95_ms = dur_quantiles[5] if len(dur_quantiles) > 5 else 0
    max_ms = dur_quantiles[6] if len(dur_quantiles) > 6 else 0

    skew_ratio = max_ms / median_ms if median_ms > 0 else 0.0

    spill_memory = stage.get("memoryBytesSpilled", 0) or 0
    spill_disk = stage.get("diskBytesSpilled", 0) or 0
    shuffle_read = stage.get("shuffleReadBytes", 0) or 0

    return {
        "stage_id": stage_id,
        "num_tasks": stage.get("numTasks", 0),
        "description": (stage.get("name") or "")[:80],
        "median_ms": median_ms,
        "p95_ms": p95_ms,
        "max_ms": max_ms,
        "skew_ratio": skew_ratio,
        "spill_memory": spill_memory,
        "spill_disk": spill_disk,
        "shuffle_read": shuffle_read,
    }


def _check_config_antipatterns(spark_props: list) -> list[str]:
    issues = []
    props = {}
    for prop in spark_props:
        key = prop[0] if isinstance(prop, list) else prop.get("key", "")
        val = prop[1] if isinstance(prop, list) else prop.get("value", "")
        props[key] = str(val)

    if props.get("spark.sql.adaptive.enabled", "true").lower() == "false":
        issues.append("AQE (Adaptive Query Execution) is DISABLED -- enable for automatic shuffle partition coalescing and join optimization")

    try:
        shuffle_parts = int(props.get("spark.sql.shuffle.partitions", "200"))
        if shuffle_parts <= 10:
            issues.append(f"spark.sql.shuffle.partitions={shuffle_parts} -- very low, may cause OOM on large shuffles")
    except ValueError:
        pass

    try:
        broadcast_mb = int(props.get("spark.sql.autoBroadcastJoinThreshold", "10485760"))
        if broadcast_mb > 1024 * 1024 * 1024:
            issues.append(f"autoBroadcastJoinThreshold is {format_bytes(broadcast_mb)} -- driver OOM risk from broadcasting large tables")
    except ValueError:
        pass

    overhead = props.get("spark.executor.memoryOverhead", "")
    try:
        if overhead.endswith("g"):
            overhead_mb = int(overhead[:-1]) * 1024
        elif overhead.endswith("m"):
            overhead_mb = int(overhead[:-1])
        elif overhead.isdigit():
            overhead_mb = int(overhead)
        else:
            overhead_mb = None
        if overhead_mb is not None and overhead_mb < 512:
            issues.append(f"spark.executor.memoryOverhead={overhead} -- may be too low for PySpark workloads (recommended: >=1g)")
    except ValueError:
        pass

    if props.get("spark.dynamicAllocation.enabled", "false").lower() == "false":
        issues.append("Dynamic allocation is disabled -- cluster won't scale down during idle periods")

    return issues
