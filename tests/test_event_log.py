"""Tests for event log reader/parser module."""

import base64
from unittest.mock import MagicMock, patch

import pytest

from databricks_debug_mcp.event_log import (
    _detect_codec,
    decompress,
    parse_events,
)


class TestDetectCodec:
    def test_zstd(self):
        assert _detect_codec("/logs/file.zstd") == "zstd"
        assert _detect_codec("/logs/file.zst") == "zstd"

    def test_lz4(self):
        assert _detect_codec("/logs/file.lz4") == "lz4"

    def test_snappy(self):
        assert _detect_codec("/logs/file.snappy") == "snappy"

    def test_plain(self):
        assert _detect_codec("/logs/file") is None
        assert _detect_codec("/logs/file.json") is None


class TestDecompress:
    def test_plain_text(self):
        data = b'{"Event": "SparkListenerJobStart"}\n'
        result = decompress(data, None)
        assert "SparkListenerJobStart" in result

    def test_unsupported_codec(self):
        with pytest.raises(ValueError, match="Unsupported"):
            decompress(b"data", "gzip")


class TestParseEvents:
    def test_basic_parsing(self):
        text = '{"Event": "SparkListenerJobStart", "Job ID": 1}\n{"Event": "SparkListenerJobEnd", "Job ID": 1}\n'
        events = parse_events(text)
        assert len(events) == 2
        assert events[0]["Event"] == "SparkListenerJobStart"

    def test_filter_by_type(self):
        text = (
            '{"Event": "SparkListenerJobStart", "Job ID": 1}\n'
            '{"Event": "SparkListenerStageCompleted", "Stage Info": {}}\n'
            '{"Event": "SparkListenerJobEnd", "Job ID": 1}\n'
        )
        events = parse_events(text, event_types={"SparkListenerStageCompleted"})
        assert len(events) == 1
        assert events[0]["Event"] == "SparkListenerStageCompleted"

    def test_skips_malformed_lines(self):
        text = '{"Event": "valid"}\nnot json\n{"Event": "also valid"}\n'
        events = parse_events(text)
        assert len(events) == 2

    def test_skips_blank_lines(self):
        text = '\n\n{"Event": "X"}\n\n'
        events = parse_events(text)
        assert len(events) == 1


class TestDiscoverEventLogPath:
    @patch("databricks_debug_mcp.event_log.get_workspace_client")
    def test_dbfs_log_path(self, mock_get_client):
        from databricks_debug_mcp.event_log import discover_event_log_path

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        cluster = MagicMock()
        cluster.cluster_log_conf.dbfs.destination = "dbfs:/cluster-logs"
        cluster.cluster_log_conf.s3 = None
        mock_client.clusters.get.return_value = cluster

        sub_entry = MagicMock()
        sub_entry.is_dir = False
        sub_entry.file_size = 1000
        sub_entry.path = "dbfs:/cluster-logs/c-1/eventlog/app-1/events_1.json.lz4"

        dir_entry = MagicMock()
        dir_entry.is_dir = True
        dir_entry.path = "dbfs:/cluster-logs/c-1/eventlog/app-1"

        mock_client.dbfs.list.side_effect = [
            [dir_entry],
            [sub_entry],
        ]

        config = discover_event_log_path("c-1")
        assert config.cluster_id == "c-1"
        assert len(config.files) == 1
        assert "events_1" in config.files[0]

    @patch("databricks_debug_mcp.event_log.get_workspace_client")
    def test_no_log_delivery(self, mock_get_client):
        from databricks_debug_mcp.event_log import discover_event_log_path

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        cluster = MagicMock()
        cluster.cluster_log_conf = None
        mock_client.clusters.get.return_value = cluster

        with pytest.raises(ValueError, match="no log delivery"):
            discover_event_log_path("c-1")

    @patch("databricks_debug_mcp.event_log.get_workspace_client")
    def test_no_files_found(self, mock_get_client):
        from databricks_debug_mcp.event_log import discover_event_log_path

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        cluster = MagicMock()
        cluster.cluster_log_conf.dbfs.destination = "dbfs:/logs"
        cluster.cluster_log_conf.s3 = None
        mock_client.clusters.get.return_value = cluster
        mock_client.dbfs.list.return_value = []

        with pytest.raises(ValueError, match="No event log files"):
            discover_event_log_path("c-1")


class TestReadDbfsFile:
    @patch("databricks_debug_mcp.event_log.get_workspace_client")
    def test_single_chunk(self, mock_get_client):
        from databricks_debug_mcp.event_log import read_dbfs_file

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        data = b"event log content"
        resp = MagicMock()
        resp.data = base64.b64encode(data).decode()
        resp.bytes_read = len(data)
        mock_client.dbfs.read.return_value = resp

        result = read_dbfs_file("dbfs:/logs/file.json")
        assert result == data

    @patch("databricks_debug_mcp.event_log.get_workspace_client")
    def test_empty_file(self, mock_get_client):
        from databricks_debug_mcp.event_log import read_dbfs_file

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        resp = MagicMock()
        resp.data = None
        mock_client.dbfs.read.return_value = resp

        result = read_dbfs_file("dbfs:/logs/empty")
        assert result == b""
