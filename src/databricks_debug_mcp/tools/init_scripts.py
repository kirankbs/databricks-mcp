import base64

from mcp.server.fastmcp import FastMCP

from ..client import get_workspace_client
from ..formatting import enum_val


def register(mcp: FastMCP) -> None:

    @mcp.tool()
    def get_init_script_logs(
        cluster_id: str,
    ) -> str:
        """Init script execution logs for a cluster — debug library installation failures, environment setup issues.

        Shows init script events from cluster history (which scripts ran, exit codes, durations),
        then reads init script log files from DBFS if log delivery is configured.
        Common issues: CRLF line endings, permission denied, missing volume paths, timeout.
        """
        w = get_workspace_client()

        try:
            cluster = w.clusters.get(cluster_id=cluster_id)
        except Exception as e:
            return f"Failed to get cluster {cluster_id}: {e}"

        lines = [f"Init script status -- cluster {cluster.cluster_name or cluster_id}:\n"]

        init_scripts = cluster.init_scripts or []
        if init_scripts:
            lines.append(f"Configured init scripts ({len(init_scripts)}):")
            for i, script in enumerate(init_scripts):
                if script.workspace:
                    lines.append(f"  [{i + 1}] workspace: {script.workspace.destination}")
                elif script.volumes:
                    lines.append(f"  [{i + 1}] volumes: {script.volumes.destination}")
                elif script.dbfs:
                    lines.append(f"  [{i + 1}] dbfs: {script.dbfs.destination}")
                elif script.abfss:
                    lines.append(f"  [{i + 1}] abfss: {script.abfss.destination}")
                elif script.gcs:
                    lines.append(f"  [{i + 1}] gcs: {script.gcs.destination}")
                elif script.s3:
                    lines.append(f"  [{i + 1}] s3: {script.s3.destination}")
                else:
                    lines.append(f"  [{i + 1}] (unknown type)")
        else:
            lines.append("No init scripts configured.")

        try:
            init_events = []
            for event in w.clusters.events(cluster_id=cluster_id, order="DESC"):
                et = enum_val(event.type)
                if "INIT_SCRIPT" in et:
                    init_events.append(event)
                if len(init_events) >= 20:
                    break

            if init_events:
                lines.append(f"\nInit script events ({len(init_events)} found):")
                for ev in reversed(init_events):
                    ts_raw = ev.timestamp
                    et = enum_val(ev.type)
                    det = ev.details
                    detail_str = ""
                    if det and det.init_scripts:
                        scripts_info = det.init_scripts
                        if hasattr(scripts_info, "num_failed_scripts"):
                            detail_str = f"failed: {scripts_info.num_failed_scripts}"
                        if hasattr(scripts_info, "execution_order"):
                            for exec_info in scripts_info.execution_order or []:
                                status = getattr(exec_info, "status", "?")
                                detail_str += f", {getattr(exec_info, 'file_name', '?')}: {status}"
                    lines.append(f"  {ts_raw}  {et}  {detail_str}")
            else:
                lines.append("\nNo init script events in cluster event log.")
        except Exception:
            lines.append("\n(Unable to fetch cluster events)")

        log_conf = cluster.cluster_log_conf
        if log_conf and log_conf.dbfs:
            base_path = log_conf.dbfs.destination.rstrip("/")
            init_log_dir = f"{base_path}/{cluster_id}/init_scripts"

            try:
                containers = w.dbfs.list(path=init_log_dir)
                container_list = list(containers) if containers else []

                if container_list:
                    lines.append(f"\nInit script log files ({len(container_list)} containers):")
                    for container in container_list[:5]:
                        container_path = container.path
                        lines.append(f"\n  Container: {container_path}")

                        try:
                            log_files = list(w.dbfs.list(path=container_path))
                            for log_file in log_files[:10]:
                                file_size = log_file.file_size or 0
                                lines.append(f"    {log_file.path} ({file_size} bytes)")

                                if file_size > 0 and file_size < 50_000:
                                    try:
                                        read_resp = w.dbfs.read(
                                            path=log_file.path,
                                            offset=0,
                                            length=min(file_size, 10_000),
                                        )
                                        content = base64.b64decode(read_resp.data or "").decode(
                                            "utf-8", errors="replace"
                                        )
                                        last_lines = content.strip().splitlines()[-20:]
                                        for line in last_lines:
                                            prefix = (
                                                ">>> "
                                                if any(
                                                    x in line.lower() for x in ["error", "fail", "denied", "not found"]
                                                )
                                                else "    "
                                            )
                                            lines.append(f"      {prefix}{line}")
                                    except Exception:
                                        lines.append("      (unable to read)")
                        except Exception:
                            lines.append("    (unable to list log files)")
            except Exception:
                lines.append(f"\nInit script logs not found at {init_log_dir}")
        else:
            lines.append("\nLog delivery not configured -- cannot read init script log files.")
            lines.append("Enable cluster_log_conf.dbfs to capture init script output.")

        return "\n".join(lines)
