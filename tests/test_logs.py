"""Tests for log tools — covers the _search_in_lines return type change."""

from unittest.mock import MagicMock, patch, PropertyMock
import base64


class TestSearchInLines:
    def test_pattern_found(self):
        from databricks_debug_mcp.tools.logs import _search_in_lines

        lines = ["INFO starting", "ERROR something broke", "INFO done"]
        found, result = _search_in_lines(lines, "ERROR", "stderr", "/logs/stderr")
        assert found is True
        assert "1 match" in result
        assert "something broke" in result

    def test_pattern_not_found(self):
        from databricks_debug_mcp.tools.logs import _search_in_lines

        lines = ["INFO all good", "INFO done"]
        found, result = _search_in_lines(lines, "ERROR", "stderr", "/logs/stderr")
        assert found is False
        assert "not found" in result.lower()

    def test_invalid_regex(self):
        from databricks_debug_mcp.tools.logs import _search_in_lines

        lines = ["test"]
        found, result = _search_in_lines(lines, "[invalid", "stderr", "/path")
        assert found is False
        assert "Invalid regex" in result

    def test_pattern_too_long(self):
        from databricks_debug_mcp.tools.logs import _search_in_lines

        lines = ["test"]
        found, result = _search_in_lines(lines, "a" * 201, "stderr", "/path")
        assert found is False
        assert "too long" in result

    def test_not_found_in_log_content_doesnt_confuse(self):
        """Regression: log line containing 'not found' shouldn't affect search logic."""
        from databricks_debug_mcp.tools.logs import _search_in_lines

        lines = ["FileNotFoundException: file not found", "ERROR crash"]
        found, result = _search_in_lines(lines, "ERROR", "stderr", "/path")
        assert found is True
        assert "1 match" in result

    def test_context_lines(self):
        from databricks_debug_mcp.tools.logs import _search_in_lines

        lines = [f"line {i}" for i in range(10)]
        lines[5] = "ERROR at line 5"
        found, result = _search_in_lines(lines, "ERROR", "stderr", "/path", context=2)
        assert found is True
        assert "line 3" in result
        assert "line 7" in result


class TestSearchLogs:
    @patch("databricks_debug_mcp.tools.logs.get_workspace_client")
    def test_search_across_log_types(self, mock_get_client):
        from databricks_debug_mcp.tools.logs import register
        from mcp.server.fastmcp import FastMCP

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        cluster = MagicMock()
        cluster.cluster_log_conf.dbfs.destination = "dbfs:/cluster-logs"
        mock_client.clusters.get.return_value = cluster

        log_content = "INFO ok\nERROR something failed\nINFO done"
        encoded = base64.b64encode(log_content.encode()).decode()

        file_info = MagicMock()
        file_info.file_size = len(log_content)
        mock_client.dbfs.get_status.return_value = file_info

        read_resp = MagicMock()
        read_resp.data = encoded
        mock_client.dbfs.read.return_value = read_resp

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["search_logs"]
        result = tool.fn(cluster_id="c-1", pattern="ERROR")
        assert "something failed" in result

    @patch("databricks_debug_mcp.tools.logs.get_workspace_client")
    def test_no_log_delivery(self, mock_get_client):
        from databricks_debug_mcp.tools.logs import register
        from mcp.server.fastmcp import FastMCP

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        cluster = MagicMock()
        cluster.cluster_log_conf = None
        mock_client.clusters.get.return_value = cluster

        mcp = FastMCP("test")
        register(mcp)
        tool = mcp._tool_manager._tools["search_logs"]
        result = tool.fn(cluster_id="c-1", pattern="ERROR")
        assert "not configured" in result.lower()
