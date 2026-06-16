"""
Unit tests for the pure helper functions in email_backfill.py.

These cover filename/path building, string sanitisation, and the date-range
math used by the historical and incremental modes. They are deliberately
offline: nothing here touches Gmail, AI Drive, or the network.

email_backfill.py validates its required environment variables at import time,
so dummy values are set below before the module is imported. They are never
used by the functions under test.

Run with:  pytest -q
"""

import os
from datetime import date, datetime

# Satisfy the module-level _require_env() checks before importing the module.
os.environ.setdefault("AIDRIVE_API_KEY", "test-key")
os.environ.setdefault("GMAIL_CLIENT_ID", "test-client-id")
os.environ.setdefault("GMAIL_CLIENT_SECRET", "test-client-secret")
os.environ.setdefault("GMAIL_REFRESH_TOKEN", "test-refresh-token")

import email_backfill as ebf  # noqa: E402


# --- sanitize_for_filename -------------------------------------------------

def test_sanitize_replaces_path_breaking_chars():
    assert ebf.sanitize_for_filename('a/b\\c:d*e?f"g<h>i|j') == "a_b_c_d_e_f_g_h_i_j"


def test_sanitize_collapses_runs_of_spaces():
    assert ebf.sanitize_for_filename("hello   world  foo") == "hello world foo"


def test_sanitize_turns_tabs_and_newlines_into_underscores():
    # \t and \n are in the replacement char class, so they become "_" before
    # the whitespace-collapsing pass runs.
    assert ebf.sanitize_for_filename("a\tb\nc") == "a_b_c"


def test_sanitize_empty_returns_placeholder():
    assert ebf.sanitize_for_filename("") == "no-subject"
    assert ebf.sanitize_for_filename(None) == "no-subject"


def test_sanitize_clips_to_max_len():
    assert ebf.sanitize_for_filename("x" * 200, max_len=80) == "x" * 80


# --- build_filename / build_folder_path ------------------------------------

def test_build_filename_format():
    d = datetime(2025, 6, 14, 9, 5)
    name = ebf.build_filename(d, "Subject", "Sender", "abcdef1234567890")
    assert name == "2025-06-14_0905_Sender_Subject_abcdef12.eml"


def test_build_folder_path_uses_year_month(monkeypatch):
    monkeypatch.setattr(ebf, "AIDRIVE_FOLDER", "04 - EMAIL ARCHIVE")
    assert ebf.build_folder_path(datetime(2025, 6, 14)) == "04 - EMAIL ARCHIVE/2025-06"


# --- parse_date_safe -------------------------------------------------------

def test_parse_date_safe_valid_header():
    parsed = ebf.parse_date_safe("Mon, 14 Jun 2025 09:05:00 +0000")
    assert (parsed.year, parsed.month, parsed.day) == (2025, 6, 14)


def test_parse_date_safe_garbage_returns_datetime():
    # Unparseable input must not raise; it falls back to "now".
    assert isinstance(ebf.parse_date_safe("not a date"), datetime)


# --- _add_months -----------------------------------------------------------

def test_add_months_within_year():
    assert ebf._add_months(date(2025, 3, 15), 2) == date(2025, 5, 1)


def test_add_months_rolls_over_year():
    assert ebf._add_months(date(2025, 11, 10), 3) == date(2026, 2, 1)


def test_add_months_negative_rolls_back_year():
    assert ebf._add_months(date(2025, 1, 10), -1) == date(2024, 12, 1)


# --- _monthly_chunks -------------------------------------------------------

def test_monthly_chunks_count_and_format():
    chunks = list(ebf._monthly_chunks(months_back=12))
    # 12 full months back plus the current partial month.
    assert len(chunks) == 13
    for start, end in chunks:
        # Gmail query format YYYY/MM/DD, start strictly before end.
        datetime.strptime(start, "%Y/%m/%d")
        datetime.strptime(end, "%Y/%m/%d")
        assert start < end


def test_monthly_chunks_last_window_ends_tomorrow():
    chunks = list(ebf._monthly_chunks(months_back=12))
    tomorrow = (date.today().toordinal() + 1)
    last_end = datetime.strptime(chunks[-1][1], "%Y/%m/%d").date()
    assert last_end.toordinal() == tomorrow


# --- _incremental_window ---------------------------------------------------

def test_incremental_window_spans_lookback_to_tomorrow(monkeypatch):
    monkeypatch.setattr(ebf, "INCREMENTAL_LOOKBACK_DAYS", 2)
    start, end = ebf._incremental_window()
    start_d = datetime.strptime(start, "%Y/%m/%d").date()
    end_d = datetime.strptime(end, "%Y/%m/%d").date()
    assert (date.today() - start_d).days == 2
    assert (end_d - date.today()).days == 1
