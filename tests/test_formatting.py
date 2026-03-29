"""Tests for formatting utilities."""

from databricks_debug_mcp.formatting import (
    deduplicate_events,
    enum_val,
    format_bytes,
    format_duration,
    ms_to_str,
    truncate_stacktrace,
)


class TestFormatDuration:
    def test_none(self):
        assert format_duration(None) == "—"

    def test_seconds(self):
        assert format_duration(5000) == "5.0s"

    def test_minutes(self):
        assert format_duration(125_000) == "2m 5s"

    def test_hours(self):
        assert format_duration(7_200_000) == "2h 0m"

    def test_zero(self):
        assert format_duration(0) == "0.0s"


class TestFormatBytes:
    def test_none(self):
        assert format_bytes(None) == "—"

    def test_bytes(self):
        assert format_bytes(500) == "500.0 B"

    def test_kilobytes(self):
        assert format_bytes(2048) == "2.0 KB"

    def test_megabytes(self):
        assert format_bytes(5 * 1024 * 1024) == "5.0 MB"

    def test_gigabytes(self):
        assert format_bytes(3 * 1024**3) == "3.0 GB"


class TestMsToStr:
    def test_none(self):
        assert ms_to_str(None) == "—"

    def test_epoch(self):
        result = ms_to_str(0)
        assert "1970-01-01" in result

    def test_valid(self):
        # 2024-01-01 00:00:00 UTC
        result = ms_to_str(1704067200000)
        assert "2024-01-01" in result


class TestTruncateStacktrace:
    def test_none(self):
        assert truncate_stacktrace(None) == ""

    def test_empty(self):
        assert truncate_stacktrace("") == ""

    def test_short(self):
        text = "line1\nline2\nline3"
        assert truncate_stacktrace(text) == text.strip()

    def test_long(self):
        lines = [f"line {i}" for i in range(50)]
        text = "\n".join(lines)
        result = truncate_stacktrace(text, max_lines=10)
        assert "omitted" in result
        assert "line 0" in result  # head preserved
        assert "line 49" in result  # tail preserved


class TestEnumVal:
    def test_none(self):
        assert enum_val(None) == "?"

    def test_string(self):
        assert enum_val("RUNNING") == "RUNNING"

    def test_enum_like(self):
        class FakeEnum:
            value = "SUCCESS"

        assert enum_val(FakeEnum()) == "SUCCESS"

    def test_fallback(self):
        assert enum_val(None, fallback="N/A") == "N/A"


class TestDeduplicateEvents:
    def test_empty(self):
        assert deduplicate_events([]) == []

    def test_no_duplicates(self):
        events = [
            {"type": "A", "timestamp": 1},
            {"type": "B", "timestamp": 2},
        ]
        result = deduplicate_events(events)
        assert len(result) == 2

    def test_consecutive_duplicates(self):
        events = [
            {"type": "A", "timestamp": 1},
            {"type": "A", "timestamp": 2},
            {"type": "A", "timestamp": 3},
            {"type": "B", "timestamp": 4},
        ]
        result = deduplicate_events(events)
        assert len(result) == 2
        assert result[0]["_repeated"] == 3
        assert result[0]["_first_timestamp"] == 1
        assert result[0]["_last_timestamp"] == 3

    def test_non_consecutive_same_type(self):
        events = [
            {"type": "A", "timestamp": 1},
            {"type": "B", "timestamp": 2},
            {"type": "A", "timestamp": 3},
        ]
        result = deduplicate_events(events)
        assert len(result) == 3
