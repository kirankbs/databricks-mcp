import httpx
from databricks.sdk import WorkspaceClient

from .config import get_config

_client: WorkspaceClient | None = None


def get_workspace_client() -> WorkspaceClient:
    global _client
    if _client is None:
        cfg = get_config()
        _client = WorkspaceClient(profile=cfg.profile)
    return _client


def _auth_headers() -> dict[str, str]:
    """Get authentication headers from the Databricks SDK."""
    w = get_workspace_client()
    return dict(w.config.authenticate())


def spark_ui_request(cluster_id: str, path: str) -> dict:
    """GET from the Spark driver proxy API. Only works on RUNNING clusters."""
    w = get_workspace_client()
    host = w.config.host.rstrip("/")
    url = f"{host}/driver-proxy-api/o/0/{cluster_id}/40001/api/v1/{path.lstrip('/')}"

    resp = httpx.get(url, headers=_auth_headers(), timeout=30.0)
    resp.raise_for_status()
    return resp.json()


def spark_ui_html(cluster_id: str, path: str) -> str:
    """GET an HTML page from the Spark UI (non-API endpoints like /StreamingQuery/)."""
    w = get_workspace_client()
    host = w.config.host.rstrip("/")
    url = f"{host}/driver-proxy-api/o/0/{cluster_id}/40001/{path.lstrip('/')}"

    resp = httpx.get(url, headers=_auth_headers(), timeout=30.0, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


def get_spark_app_id(cluster_id: str) -> str:
    """Discover the Spark application ID for a running cluster."""
    data = spark_ui_request(cluster_id, "applications")
    if not data:
        raise ValueError(f"No Spark applications found for cluster {cluster_id}")
    return data[0]["id"]


_CLUSTER_TERMINATED_MSG = (
    "Cluster {cluster_id} is not RUNNING -- Spark UI data is unavailable for terminated clusters.\n"
    "Alternatives:\n"
    "  - get_run_details: task timings and statuses\n"
    "  - get_driver_logs: driver log content\n"
    "  - get_cluster_events: infrastructure events (DRIVER_NOT_RESPONDING, etc.)"
)


def assert_cluster_running(cluster_id: str) -> str | None:
    """Return an error string if the cluster is not RUNNING, else None."""
    from .formatting import enum_val
    w = get_workspace_client()
    try:
        cluster = w.clusters.get(cluster_id=cluster_id)
    except Exception as e:
        return f"Failed to get cluster {cluster_id}: {e}"
    state = enum_val(cluster.state)
    if state != "RUNNING":
        return _CLUSTER_TERMINATED_MSG.format(cluster_id=cluster_id)
    return None
