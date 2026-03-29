"""Tests for the Databricks client module."""

from unittest.mock import MagicMock, patch

import pytest


class TestGetWorkspaceClient:
    @patch("databricks_debug_mcp.client.WorkspaceClient")
    @patch("databricks_debug_mcp.client.get_config")
    @patch("databricks_debug_mcp.client._client", None)
    def test_creates_client_with_profile(self, mock_config, mock_ws):
        from databricks_debug_mcp.client import get_workspace_client

        mock_config.return_value = MagicMock(profile="STAGING")
        client = get_workspace_client()

        mock_ws.assert_called_once_with(profile="STAGING")
        assert client is mock_ws.return_value

    @patch("databricks_debug_mcp.client.WorkspaceClient")
    @patch("databricks_debug_mcp.client.get_config")
    @patch("databricks_debug_mcp.client._client", None)
    def test_singleton_returns_same_instance(self, mock_config, mock_ws):
        from databricks_debug_mcp.client import get_workspace_client

        mock_config.return_value = MagicMock(profile="TEST")
        first = get_workspace_client()
        second = get_workspace_client()

        assert first is second
        mock_ws.assert_called_once()


class TestAuthHeaders:
    @patch("databricks_debug_mcp.client.get_workspace_client")
    def test_returns_auth_dict(self, mock_get_client):
        from databricks_debug_mcp.client import _auth_headers

        mock_client = MagicMock()
        mock_client.config.authenticate.return_value = [
            ("Authorization", "Bearer tok123"),
        ]
        mock_get_client.return_value = mock_client

        headers = _auth_headers()
        assert headers == {"Authorization": "Bearer tok123"}


class TestSparkUiRequest:
    @patch("databricks_debug_mcp.client._auth_headers", return_value={"Authorization": "Bearer t"})
    @patch("databricks_debug_mcp.client.httpx.get")
    @patch("databricks_debug_mcp.client.get_workspace_client")
    def test_returns_json(self, mock_get_client, mock_httpx_get, mock_auth):
        from databricks_debug_mcp.client import spark_ui_request

        mock_client = MagicMock()
        mock_client.config.host = "https://myws.databricks.com"
        mock_get_client.return_value = mock_client

        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"id": "app-1"}]
        mock_httpx_get.return_value = mock_resp

        result = spark_ui_request("c-123", "applications")
        assert result == [{"id": "app-1"}]
        mock_resp.raise_for_status.assert_called_once()

    @patch("databricks_debug_mcp.client._auth_headers", return_value={"Authorization": "Bearer t"})
    @patch("databricks_debug_mcp.client.httpx.get")
    @patch("databricks_debug_mcp.client.get_workspace_client")
    def test_url_construction(self, mock_get_client, mock_httpx_get, mock_auth):
        from databricks_debug_mcp.client import spark_ui_request

        mock_client = MagicMock()
        mock_client.config.host = "https://myws.databricks.com/"
        mock_get_client.return_value = mock_client

        mock_httpx_get.return_value = MagicMock()
        spark_ui_request("c-123", "applications/app-1/stages")

        url = mock_httpx_get.call_args[0][0]
        assert url == "https://myws.databricks.com/driver-proxy-api/o/0/c-123/40001/api/v1/applications/app-1/stages"

    @patch("databricks_debug_mcp.client._auth_headers", return_value={})
    @patch("databricks_debug_mcp.client.httpx.get")
    @patch("databricks_debug_mcp.client.get_workspace_client")
    def test_http_error_propagated(self, mock_get_client, mock_httpx_get, mock_auth):
        import httpx

        from databricks_debug_mcp.client import spark_ui_request

        mock_client = MagicMock()
        mock_client.config.host = "https://myws.databricks.com"
        mock_get_client.return_value = mock_client

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError("404", request=MagicMock(), response=MagicMock())
        mock_httpx_get.return_value = mock_resp

        with pytest.raises(httpx.HTTPStatusError):
            spark_ui_request("c-123", "applications")


class TestSparkUiHtml:
    @patch("databricks_debug_mcp.client._auth_headers", return_value={"Authorization": "Bearer t"})
    @patch("databricks_debug_mcp.client.httpx.get")
    @patch("databricks_debug_mcp.client.get_workspace_client")
    def test_returns_text(self, mock_get_client, mock_httpx_get, mock_auth):
        from databricks_debug_mcp.client import spark_ui_html

        mock_client = MagicMock()
        mock_client.config.host = "https://myws.databricks.com"
        mock_get_client.return_value = mock_client

        mock_resp = MagicMock()
        mock_resp.text = "<html><body>Streaming</body></html>"
        mock_httpx_get.return_value = mock_resp

        result = spark_ui_html("c-123", "StreamingQuery/")
        assert result == "<html><body>Streaming</body></html>"

    @patch("databricks_debug_mcp.client._auth_headers", return_value={})
    @patch("databricks_debug_mcp.client.httpx.get")
    @patch("databricks_debug_mcp.client.get_workspace_client")
    def test_url_has_no_api_prefix(self, mock_get_client, mock_httpx_get, mock_auth):
        from databricks_debug_mcp.client import spark_ui_html

        mock_client = MagicMock()
        mock_client.config.host = "https://myws.databricks.com"
        mock_get_client.return_value = mock_client

        mock_httpx_get.return_value = MagicMock()
        spark_ui_html("c-123", "StreamingQuery/")

        url = mock_httpx_get.call_args[0][0]
        assert "/api/v1/" not in url
        assert "StreamingQuery/" in url

    @patch("databricks_debug_mcp.client._auth_headers", return_value={})
    @patch("databricks_debug_mcp.client.httpx.get")
    @patch("databricks_debug_mcp.client.get_workspace_client")
    def test_follow_redirects(self, mock_get_client, mock_httpx_get, mock_auth):
        from databricks_debug_mcp.client import spark_ui_html

        mock_client = MagicMock()
        mock_client.config.host = "https://myws.databricks.com"
        mock_get_client.return_value = mock_client

        mock_httpx_get.return_value = MagicMock()
        spark_ui_html("c-123", "StreamingQuery/")

        assert mock_httpx_get.call_args[1]["follow_redirects"] is True


class TestGetSparkAppId:
    @patch("databricks_debug_mcp.client.spark_ui_request")
    def test_returns_first_app(self, mock_request):
        from databricks_debug_mcp.client import get_spark_app_id

        mock_request.return_value = [{"id": "app-12345"}]
        assert get_spark_app_id("c-1") == "app-12345"

    @patch("databricks_debug_mcp.client.spark_ui_request")
    def test_no_apps_raises(self, mock_request):
        from databricks_debug_mcp.client import get_spark_app_id

        mock_request.return_value = []
        with pytest.raises(ValueError, match="No Spark applications"):
            get_spark_app_id("c-1")


class TestAssertClusterRunning:
    @patch("databricks_debug_mcp.client.get_workspace_client")
    def test_running_returns_none(self, mock_get_client):
        from databricks_debug_mcp.client import assert_cluster_running

        mock_client = MagicMock()
        cluster = MagicMock()
        cluster.state = MagicMock(value="RUNNING")
        mock_client.clusters.get.return_value = cluster
        mock_get_client.return_value = mock_client

        assert assert_cluster_running("c-1") is None

    @patch("databricks_debug_mcp.client.get_workspace_client")
    def test_terminated_returns_error(self, mock_get_client):
        from databricks_debug_mcp.client import assert_cluster_running

        mock_client = MagicMock()
        cluster = MagicMock()
        cluster.state = MagicMock(value="TERMINATED")
        mock_client.clusters.get.return_value = cluster
        mock_get_client.return_value = mock_client

        result = assert_cluster_running("c-1")
        assert result is not None
        assert "not RUNNING" in result
        assert "get_driver_logs" in result

    @patch("databricks_debug_mcp.client.get_workspace_client")
    def test_get_cluster_fails(self, mock_get_client):
        from databricks_debug_mcp.client import assert_cluster_running

        mock_client = MagicMock()
        mock_client.clusters.get.side_effect = Exception("Cluster not found")
        mock_get_client.return_value = mock_client

        result = assert_cluster_running("c-bad")
        assert "Failed to get cluster" in result
