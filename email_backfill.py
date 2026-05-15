"""
email_backfill.py

Pulls emails from Gmail and uploads each one as a .eml file to AI Drive
via the AI Drive API (signed-URL upload flow).

Designed to run unattended in GitHub Actions, but also runs locally.

Idempotent: applies a Gmail label "aidrive-archived" to each successfully
uploaded message, so reruns of the same date range skip already-processed
messages — no duplicates.

ATTACHMENT HANDLING:
  Gmail's "raw" format returns the complete RFC 822 message bytes, which
  includes all MIME parts (body text, HTML alternatives, and every attachment).
  When the script uploads this as a .eml file, attachments are already embedded
  inside the payload. No separate attachment handling is required.

OPERATING MODES (set via RUN_MODE environment variable):

  historical   Automatically works through the last 12 months one calendar
               month at a time. No START_DATE/END_DATE needed. Use this for
               the initial catch-up or to fill any historical gaps.

  incremental  Syncs the most recent emails (last INCREMENTAL_LOOKBACK_DAYS
               days, default 2). Designed for the scheduled 30-minute run.
               The label-based deduplication prevents double-uploads even
               though the window is wider than 30 minutes.

  (not set)    Falls back to custom mode: START_DATE and END_DATE must be
               provided as YYYY/MM/DD values. Behaves exactly as before for
               manual one-off runs.

ENVIRONMENT VARIABLES:

  AIDRIVE_API_KEY              your AI Drive API key
  AIDRIVE_FOLDER               destination folder, e.g. "04 - EMAIL ARCHIVE"
  GMAIL_CLIENT_ID              OAuth client id from Google Cloud Console
  GMAIL_CLIENT_SECRET          OAuth client secret
  GMAIL_REFRESH_TOKEN          refresh token obtained via get_gmail_token.py
  RUN_MODE                     "historical" | "incremental" | (empty = custom)
  START_DATE                   YYYY/MM/DD — required only in custom mode
  END_DATE                     YYYY/MM/DD — required only in custom mode
  MAX_EMAILS                   (optional) per-window cap on emails processed; in historical
                               mode this limit is applied independently to each monthly
                               chunk, so the total across all chunks can be much higher.
                               Default 2000.
  INCREMENTAL_LOOKBACK_DAYS    (optional) days back for incremental, default 2

USAGE:
  python email_backfill.py
"""

import base64
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# === Config ===
AIDRIVE_BASE = "https://ai-drive-api-prod-qvg2narjsa-uc.a.run.app"
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]
PROCESSED_LABEL = "aidrive-archived"
BATCH_SIZE = 25                # emails per signed-url batch
MAX_RETRIES = 3
RETRY_DELAY_SECS = 5
REQUEST_TIMEOUT_SECS = 60
UPLOAD_TIMEOUT_SECS = 120

# === Environment ===
def _require_env(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            "Check your GitHub Actions secrets configuration."
        )
    return value


AIDRIVE_API_KEY = _require_env("AIDRIVE_API_KEY")
AIDRIVE_FOLDER = os.environ.get("AIDRIVE_FOLDER", "04 - EMAIL ARCHIVE")
GMAIL_CLIENT_ID = _require_env("GMAIL_CLIENT_ID")
GMAIL_CLIENT_SECRET = _require_env("GMAIL_CLIENT_SECRET")
GMAIL_REFRESH_TOKEN = _require_env("GMAIL_REFRESH_TOKEN")
# RUN_MODE: "historical" | "incremental" | "" (empty = custom, requires START/END)
RUN_MODE = os.environ.get("RUN_MODE", "").strip().lower()
START_DATE = os.environ.get("START_DATE", "")
END_DATE = os.environ.get("END_DATE", "")
MAX_EMAILS = int(os.environ.get("MAX_EMAILS", "2000"))
# For incremental mode: how many calendar days back to query (2 days catches
# all mail that arrived since the previous 30-minute run, even across midnight).
INCREMENTAL_LOOKBACK_DAYS = int(os.environ.get("INCREMENTAL_LOOKBACK_DAYS", "2"))


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


def get_gmail_service():
    creds = Credentials(
        token=None,
        refresh_token=GMAIL_REFRESH_TOKEN,
        client_id=GMAIL_CLIENT_ID,
        client_secret=GMAIL_CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=GMAIL_SCOPES,
    )
    creds.refresh(GoogleRequest())
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _explain_http_error_and_exit(err):
    """Detects common, actionable Gmail API errors and exits with guidance."""
    status = getattr(getattr(err, "resp", None), "status", None)
    body = ""
    try:
        body = err.content.decode("utf-8", errors="ignore") if err.content else ""
    except Exception:
        body = str(err)

    # Gmail API not enabled in the GCP project that owns the OAuth client.
    if status == 403 and "accessNotConfigured" in body:
        # Try to surface the project number from the error body so the user
        # knows exactly which project to enable the API in.
        project_match = re.search(r"project[s]?[/=](\d+)", body)
        project_hint = (
            f" (project {project_match.group(1)})" if project_match else ""
        )
        log("ERROR: Gmail API is not enabled for this Google Cloud project"
            f"{project_hint}.")
        log("       Enable it at: "
            "https://console.developers.google.com/apis/api/gmail.googleapis.com/overview"
            + (f"?project={project_match.group(1)}" if project_match else ""))
        log("       After enabling, wait 1–2 minutes for propagation and re-run.")
        sys.exit(1)

    if status == 401:
        log("ERROR: Gmail rejected the OAuth credentials (HTTP 401). "
            "Regenerate GMAIL_REFRESH_TOKEN with get_gmail_token.py and ensure "
            "GMAIL_CLIENT_ID / GMAIL_CLIENT_SECRET match the same OAuth client.")
        sys.exit(1)


def ensure_processed_label(service):
    """Returns the Gmail label id for aidrive-archived, creating it if needed.

    The first Gmail call is wrapped in a bounded retry loop so transient
    issues (notably the brief propagation delay right after enabling the
    Gmail API in the GCP console) don't fail the whole run. Permanent,
    actionable errors are translated to a clear message and a fast exit.
    """
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            labels = (
                service.users().labels().list(userId="me").execute().get("labels", [])
            )
            for lbl in labels:
                if lbl["name"] == PROCESSED_LABEL:
                    return lbl["id"]
            created = service.users().labels().create(
                userId="me",
                body={
                    "name": PROCESSED_LABEL,
                    "labelListVisibility": "labelShow",
                    "messageListVisibility": "show",
                },
            ).execute()
            return created["id"]
        except HttpError as e:
            last_err = e
            status = getattr(getattr(e, "resp", None), "status", None)
            # Don't retry permanent client errors except 403
            # accessNotConfigured, which can be transient right after the
            # API is enabled in the GCP console.
            body = ""
            try:
                body = e.content.decode("utf-8", errors="ignore") if e.content else ""
            except Exception:
                pass
            transient = (
                status is None
                or status >= 500
                or status == 429
                or (status == 403 and "accessNotConfigured" in body)
            )
            if not transient or attempt == MAX_RETRIES - 1:
                _explain_http_error_and_exit(e)
                raise
            log(
                f"ensure_processed_label attempt {attempt + 1} failed "
                f"(HTTP {status}); retrying in {RETRY_DELAY_SECS}s..."
            )
            time.sleep(RETRY_DELAY_SECS)
    # Should be unreachable, but keep a safe fallback.
    raise RuntimeError(
        f"ensure_processed_label failed after {MAX_RETRIES} retries: {last_err}"
    )


def sanitize_for_filename(text, max_len=80):
    """Strip path-breaking chars and clip length.

    Kept for backwards compatibility; new code should use
    :func:`sanitize_filename`, which is stricter and matches AI Drive's
    rules for ``drive_object.name``.
    """
    if not text:
        return "no-subject"
    cleaned = re.sub(r'[\/\\:*?"<>|\r\n\t]', "_", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:max_len] or "no-subject"


# ---------------------------------------------------------------------------
# AI Drive filename / extension sanitization
# ---------------------------------------------------------------------------
#
# AI Drive's signed_url_upload_batch_v2 endpoint validates drive_object.name
# server-side and rejects records that contain certain characters or that
# carry an unsupported extension. The helpers below produce names that pass
# that validation and never send unknown extensions to AI Drive.

# Characters AI Drive rejects in drive_object.name. Stripped (replaced with a
# space and then collapsed) rather than substituted with "_" so the resulting
# name stays readable, e.g. "Lowered Prices [!] Most-Sold Styles [!!] 48
# Hours Only" -> "Lowered Prices Most-Sold Styles 48 Hours Only".
_INVALID_FILENAME_CHARS_RE = re.compile(r'[\[\]!:?*<>|"\\/]')
# Other control / whitespace characters that should never appear in a name.
_CONTROL_WS_RE = re.compile(r"[\r\n\t\f\v\x00-\x1f]")
# Maximum length of the *base* (pre-extension) part of the filename. Keeps
# the full name comfortably under typical 255-byte filesystem limits even
# after the timestamp/sender/msg-id prefix is added.
_MAX_FILENAME_BASE_LEN = 120

# Extensions AI Drive accepts. ``.eml`` is included because this archiver
# uploads RFC 822 messages. Keep lowercase, leading dot.
ALLOWED_EXTENSIONS = frozenset({
    ".pdf", ".csv", ".xlsx", ".xls", ".docx", ".doc",
    ".pptx", ".ppt", ".txt", ".md", ".eml",
})

# MIME → extension hints used when a filename has no usable extension.
_MIME_TO_EXT = {
    "application/pdf": ".pdf",
    "text/csv": ".csv",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
    "application/vnd.ms-powerpoint": ".ppt",
    "text/plain": ".txt",
    "text/markdown": ".md",
    "message/rfc822": ".eml",
}

FALLBACK_EXTENSION = ".txt"


def sanitize_filename(text, max_len=_MAX_FILENAME_BASE_LEN):
    """Return an AI-Drive-safe version of ``text`` for ``drive_object.name``.

    * Removes characters AI Drive rejects: ``[ ] ! : ? * < > | " \\ /``
    * Strips control characters
    * Collapses any run of whitespace into a single space
    * Trims leading/trailing whitespace and dots (Windows-hostile)
    * Safely truncates the result to ``max_len`` characters while
      preserving readability (prefers a word boundary)
    * Returns a stable placeholder if the input becomes empty
    """
    if text is None:
        return "untitled"
    cleaned = _CONTROL_WS_RE.sub(" ", str(text))
    cleaned = _INVALID_FILENAME_CHARS_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    if not cleaned:
        return "untitled"
    if len(cleaned) > max_len:
        # Truncate on a word boundary when possible, fall back to a hard cut.
        cut = cleaned[:max_len].rstrip()
        space = cut.rfind(" ")
        if space >= max_len - 20 and space > 0:
            cut = cut[:space].rstrip()
        cleaned = cut.rstrip(" .") or cleaned[:max_len]
    return cleaned or "untitled"


def normalize_extension(filename, mime_type=None):
    """Return a valid AI-Drive extension (including the leading dot).

    * If ``filename`` already ends with an allowed extension, that
      extension is returned (lowercased).
    * Otherwise ``mime_type`` is consulted via the MIME → extension map.
    * If neither yields an allowed extension, :data:`FALLBACK_EXTENSION`
      (``.txt``) is returned so the upload still has a valid extension.
    """
    if filename:
        # Use the last dot so names like "foo.bar.pdf" resolve to ".pdf".
        dot = filename.rfind(".")
        if 0 < dot < len(filename) - 1:
            ext = filename[dot:].lower()
            if (
                ext in ALLOWED_EXTENSIONS
                and not _INVALID_FILENAME_CHARS_RE.search(ext)
                and " " not in ext
            ):
                return ext
    if mime_type:
        mapped = _MIME_TO_EXT.get(mime_type.split(";", 1)[0].strip().lower())
        if mapped in ALLOWED_EXTENSIONS:
            return mapped
    return FALLBACK_EXTENSION


def validate_drive_object(drive_object):
    """Validate a ``drive_object`` payload before sending it to AI Drive.

    Returns ``(is_valid, reason)``. ``reason`` is the empty string when
    valid. Permanent rejections (bad/missing name, bad extension, missing
    path) should cause the caller to skip the record cleanly without
    retrying — AI Drive will reject the same payload on every attempt.
    """
    if not isinstance(drive_object, dict):
        return False, "drive_object is not a dict"
    name = drive_object.get("name")
    if not name or not isinstance(name, str):
        return False, "missing or empty name"
    if _INVALID_FILENAME_CHARS_RE.search(name):
        return False, "name contains invalid characters"
    if _CONTROL_WS_RE.search(name):
        return False, "name contains control characters"
    if name.strip() != name or name.endswith("."):
        return False, "name has stray whitespace or trailing dot"
    if len(name) > 255:
        return False, "name exceeds 255 characters"
    dot = name.rfind(".")
    if dot <= 0 or dot == len(name) - 1:
        return False, "name is missing an extension"
    ext = name[dot:].lower()
    if ext not in ALLOWED_EXTENSIONS:
        return False, f"extension {ext!r} is not in ALLOWED_EXTENSIONS"
    if not drive_object.get("path"):
        return False, "missing path"
    return True, ""


def parse_date_safe(date_header):
    try:
        return parsedate_to_datetime(date_header)
    except Exception:
        return datetime.now(timezone.utc)


def list_message_ids(service, query):
    """Yields message ids for the query, paginated."""
    page_token = None
    fetched = 0
    while True:
        resp = service.users().messages().list(
            userId="me",
            q=query,
            pageToken=page_token,
            maxResults=500,
        ).execute()
        for m in resp.get("messages", []):
            yield m["id"]
            fetched += 1
            if fetched >= MAX_EMAILS:
                return
        page_token = resp.get("nextPageToken")
        if not page_token:
            return


def fetch_raw_email(service, msg_id):
    """Returns (raw_bytes, parsed_date, subject_safe, from_safe, subject_raw)."""
    msg = service.users().messages().get(
        userId="me",
        id=msg_id,
        format="raw",
    ).execute()
    raw_bytes = base64.urlsafe_b64decode(msg["raw"])

    # Parse minimal headers from the raw bytes for naming
    headers_text = raw_bytes.split(b"\r\n\r\n", 1)[0].decode("utf-8", errors="ignore")
    headers = {}
    for line in headers_text.splitlines():
        if ":" in line and not line.startswith(" "):
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()

    parsed_date = parse_date_safe(headers.get("date", ""))
    subject_raw = headers.get("subject", "")
    subject_safe = (
        sanitize_filename(subject_raw, max_len=80) if subject_raw else "no-subject"
    )
    from_raw = headers.get("from", "")
    from_name = from_raw.split("<")[0].strip() or from_raw.strip() or "unknown-sender"
    from_safe = sanitize_filename(from_name, max_len=40)
    return raw_bytes, parsed_date, subject_safe, from_safe, subject_raw


def build_filename(parsed_date, subject_safe, from_safe, msg_id,
                   mime_type="message/rfc822"):
    """Build an AI-Drive-safe filename with a guaranteed-valid extension."""
    base = (
        f"{parsed_date.strftime('%Y-%m-%d_%H%M')}_"
        f"{from_safe}_{subject_safe}_{msg_id[:8]}"
    )
    base = sanitize_filename(base, max_len=_MAX_FILENAME_BASE_LEN)
    ext = normalize_extension(base, mime_type=mime_type)
    return f"{base}{ext}"


def build_folder_path(parsed_date):
    return f"{AIDRIVE_FOLDER}/{parsed_date.strftime('%Y-%m')}"


def aidrive_headers():
    return {
        "Authorization": f"Bearer {AIDRIVE_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _post_with_retries(url, json_body, label):
    """POST JSON to ``url`` with retries. Returns parsed JSON on success or
    raises RuntimeError after exhausting retries. Network/transport exceptions
    are caught and treated as retryable."""
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(
                url,
                headers=aidrive_headers(),
                json=json_body,
                timeout=REQUEST_TIMEOUT_SECS,
            )
            if r.status_code == 200:
                return r.json()
            last_err = f"HTTP {r.status_code} {r.text[:200]}"
        except requests.RequestException as e:
            last_err = f"network error: {e}"
        log(f"{label} attempt {attempt + 1} failed: {last_err}")
        time.sleep(RETRY_DELAY_SECS)
    raise RuntimeError(f"{label} failed after {MAX_RETRIES} retries: {last_err}")


def request_signed_urls(file_batch):
    """
    file_batch: list of dicts with keys: name, path, size
    Returns: list of SignedUrlResponseV2 items (one-to-one with file_batch).
    Raises RuntimeError if the API does not return one response per request.
    """
    body = {
        "files": [
            {
                "drive_object": {
                    "name": f["name"],
                    "path": f["path"],
                    "isFile": True,
                    "file_type": "eml",
                },
                "size": f["size"],
            }
            for f in file_batch
        ]
    }
    resp = _post_with_retries(
        f"{AIDRIVE_BASE}/signed_url_upload_batch_v2",
        body,
        "signed_url_upload_batch_v2",
    )
    # Response shape can be a bare list or wrapped in a key. Normalize.
    if isinstance(resp, dict):
        for key in ("files", "results", "items", "signed_urls"):
            if key in resp and isinstance(resp[key], list):
                resp = resp[key]
                break
    if not isinstance(resp, list):
        raise RuntimeError(
            f"signed_url_upload_batch_v2 returned unexpected payload type: "
            f"{type(resp).__name__}"
        )
    if len(resp) != len(file_batch):
        raise RuntimeError(
            f"signed_url_upload_batch_v2 returned {len(resp)} entries for "
            f"{len(file_batch)} requested files"
        )
    return resp


def upload_to_gcs(signed_url_obj, raw_bytes):
    """PUT the bytes to Google Cloud Storage using the signed URL."""
    url = signed_url_obj["url"]
    extra_headers = signed_url_obj.get("headers") or {}
    headers = {"Content-Type": "message/rfc822"}
    headers.update(extra_headers)
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.put(
                url, data=raw_bytes, headers=headers, timeout=UPLOAD_TIMEOUT_SECS
            )
            if r.status_code in (200, 201):
                return True
            last_err = f"HTTP {r.status_code} {r.text[:200]}"
        except requests.RequestException as e:
            last_err = f"network error: {e}"
        log(f"GCS PUT attempt {attempt + 1} failed: {last_err}")
        time.sleep(RETRY_DELAY_SECS)
    return False


def register_upload(drive_object, signed_url_str, size_mb, success, duration):
    body = {
        "drive_object": drive_object,
        "signedUrl": signed_url_str,
        "fileSizeMb": size_mb,
        "uploadSuccess": success,
        "uploadTimeDurationSecs": duration,
    }
    try:
        _post_with_retries(
            f"{AIDRIVE_BASE}/file_upload_status_v2",
            body,
            "file_upload_status_v2",
        )
        return True
    except RuntimeError as e:
        log(f"register_upload failed: {e}")
        return False


def label_message_processed(service, msg_id, label_id):
    service.users().messages().modify(
        userId="me",
        id=msg_id,
        body={"addLabelIds": [label_id]},
    ).execute()


# ---------------------------------------------------------------------------
# Date-range helpers
# ---------------------------------------------------------------------------

def _add_months(d, months):
    """Return the first day of the month `months` ahead of `d`."""
    m = d.month - 1 + months
    y = d.year + m // 12
    m = m % 12 + 1
    return date(y, m, 1)


def _monthly_chunks(months_back=12):
    """Yield (start_str, end_str) pairs covering the last `months_back` months.

    Each pair is one calendar month expressed in Gmail query format YYYY/MM/DD.
    The final chunk ends on tomorrow to include today's mail.
    """
    today = date.today()
    current = _add_months(date(today.year, today.month, 1), -months_back)
    while True:
        next_m = _add_months(current, 1)
        # Cap the end at tomorrow so we never query beyond today
        end = min(next_m, today + timedelta(days=1))
        yield current.strftime("%Y/%m/%d"), end.strftime("%Y/%m/%d")
        if next_m > today:
            break
        current = next_m


def _incremental_window():
    """Return (start_str, end_str) for the incremental lookback window.

    Gmail's after:/before: filters are date-granular (YYYY/MM/DD), not
    time-granular. We query the last INCREMENTAL_LOOKBACK_DAYS calendar days
    to ensure we catch all mail that arrived since the previous scheduled run,
    even across midnight boundaries. The aidrive-archived label prevents
    re-uploading messages that were already processed.
    """
    today = date.today()
    start = today - timedelta(days=INCREMENTAL_LOOKBACK_DAYS)
    end = today + timedelta(days=1)
    return start.strftime("%Y/%m/%d"), end.strftime("%Y/%m/%d")


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def process_window(service, label_id, start_str, end_str):
    """Upload all unlabelled emails in [start_str, end_str) to AI Drive.

    Returns (successes, failures, candidates) counts.
    Emails are fetched as raw RFC 822 bytes (.eml), which include the full
    message body AND all attachments embedded in the MIME structure — no
    separate attachment handling is needed.
    """
    query = f"after:{start_str} before:{end_str} -label:{PROCESSED_LABEL}"
    log(f"  Gmail query: {query}")

    msg_ids = list(list_message_ids(service, query))
    log(f"  Found {len(msg_ids)} candidate email(s) in {start_str} – {end_str}")

    successes, failures, skipped = 0, 0, 0
    batch = []  # list of dicts: msg_id, raw_bytes, drive_object, size_bytes, size_mb

    def flush_batch():
        nonlocal successes, failures
        if not batch:
            return

        # Belt-and-braces: re-validate every drive_object inside the batch.
        # The per-message loop already validates and skips invalid records,
        # but this guard ensures we never send an unknown extension or
        # invalid name to signed_url_upload_batch_v2 even if a future code
        # path forgets the upfront check.
        valid_items = []
        for item in batch:
            ok, reason = validate_drive_object(item["drive_object"])
            if not ok:
                log(
                    "  Drop from batch msg_id=%s name=%r reason=%r"
                    % (item["msg_id"], item["drive_object"].get("name"), reason)
                )
                failures += 1
                continue
            valid_items.append(item)
        batch[:] = valid_items
        if not batch:
            return

        batch_meta = [
            {
                "name": item["drive_object"]["name"],
                "path": item["drive_object"]["path"],
                "size": item["size_bytes"],
            }
            for item in batch
        ]

        try:
            signed_responses = request_signed_urls(batch_meta)
        except Exception as e:
            log(f"  Batch signed-URL request failed: {e}. Skipping batch.")
            failures += len(batch)
            batch.clear()
            return

        for item, signed_resp in zip(batch, signed_responses):
            if signed_resp.get("error") or not signed_resp.get("signed_url"):
                log(
                    f"  Skip {item['drive_object']['name']}: "
                    f"{signed_resp.get('error')}"
                )
                failures += 1
                continue

            signed_obj = signed_resp["signed_url"]
            t0 = time.time()
            ok = upload_to_gcs(signed_obj, item["raw_bytes"])
            duration = time.time() - t0
            if not ok:
                failures += 1
                continue

            # Upload to GCS succeeded. Label the message immediately so a
            # rerun does not re-upload it even if registration below fails.
            try:
                label_message_processed(service, item["msg_id"], label_id)
            except Exception as e:
                log(f"  Warning: could not label {item['msg_id']}: {e}")

            registered = register_upload(
                drive_object=signed_resp["drive_object"],
                signed_url_str=signed_obj["url"],
                size_mb=item["size_mb"],
                success=True,
                duration=duration,
            )
            if not registered:
                # The bytes are in GCS and the message is labeled; AI Drive
                # registration failed. Surface as failure but do not retry
                # (would create a duplicate upload).
                log(
                    f"  Warning: upload OK but registration failed for "
                    f"{item['drive_object']['name']}"
                )
                failures += 1
                continue

            successes += 1
            log(f"  OK ({successes}): {item['drive_object']['name']}")

        batch.clear()

    for i, msg_id in enumerate(msg_ids, start=1):
        try:
            raw_bytes, parsed_date, subject_safe, from_safe, subject_raw = (
                fetch_raw_email(service, msg_id)
            )
        except Exception as e:
            log(f"  Error fetching {msg_id}: {e}")
            failures += 1
            continue

        # Emails are uploaded as RFC 822 .eml; record the MIME type for logs
        # and so normalize_extension() has a sensible fallback.
        mime_type = "message/rfc822"
        original_filename = (
            f"{subject_raw}.eml" if subject_raw else f"{msg_id[:8]}.eml"
        )
        filename = build_filename(
            parsed_date, subject_safe, from_safe, msg_id, mime_type=mime_type
        )
        folder_path = build_folder_path(parsed_date)
        size_bytes = len(raw_bytes)
        size_mb = size_bytes / (1024 * 1024)
        final_ext = normalize_extension(filename, mime_type=mime_type)

        drive_object = {
            "name": filename,
            "path": folder_path,
            "isFile": True,
            "file_type": final_ext.lstrip("."),
        }

        # Validate the payload before sending it to signed_url_upload_batch_v2.
        # Permanent rejections are skipped cleanly so AI Drive never sees an
        # invalid payload and we never retry the same bad record.
        is_valid, reason = validate_drive_object(drive_object)
        if not is_valid:
            skipped += 1
            log(
                "  Skip msg_id=%s reason=%r original=%r sanitized=%r "
                "mime=%s ext=%s" % (
                    msg_id, reason, original_filename, filename,
                    mime_type, final_ext,
                )
            )
            continue

        if filename != original_filename:
            log(
                "  Sanitized msg_id=%s original=%r sanitized=%r "
                "mime=%s ext=%s" % (
                    msg_id, original_filename, filename, mime_type, final_ext,
                )
            )

        batch.append(
            {
                "msg_id": msg_id,
                "raw_bytes": raw_bytes,
                "drive_object": drive_object,
                "size_bytes": size_bytes,
                "size_mb": size_mb,
            }
        )

        if len(batch) >= BATCH_SIZE:
            flush_batch()

        if i % 100 == 0:
            log(
                f"  Progress: {i}/{len(msg_ids)} "
                f"(successes: {successes}, failures: {failures}, "
                f"skipped: {skipped})"
            )

    flush_batch()
    if skipped:
        log(f"  Skipped {skipped} message(s) due to validation failures")
    return successes, failures, len(msg_ids)


def main():
    log("=" * 60)

    if RUN_MODE == "historical":
        # -------------------------------------------------------------------
        # HISTORICAL mode: automatically process the last 12 months,
        # one calendar month at a time.
        # -------------------------------------------------------------------
        log("MODE: historical — processing last 12 months month by month")
        log(f"Folder: {AIDRIVE_FOLDER}. Per-chunk cap: {MAX_EMAILS}")
        service = get_gmail_service()
        label_id = ensure_processed_label(service)
        log(f"Gmail label '{PROCESSED_LABEL}' id: {label_id}")

        chunks = list(_monthly_chunks(months_back=12))
        log(f"Total chunks to process: {len(chunks)}")

        total_successes, total_failures, total_candidates = 0, 0, 0
        for idx, (start_str, end_str) in enumerate(chunks, start=1):
            log(f"\n[Chunk {idx}/{len(chunks)}] {start_str} → {end_str}")
            s, f, c = process_window(service, label_id, start_str, end_str)
            total_successes += s
            total_failures += f
            total_candidates += c
            log(
                f"[Chunk {idx}/{len(chunks)}] done — "
                f"successes: {s}, failures: {f}, candidates: {c}"
            )

        log(
            f"\nHistorical backfill complete. "
            f"Total successes: {total_successes}, "
            f"failures: {total_failures}, "
            f"candidates: {total_candidates}"
        )
        if total_failures > 0:
            sys.exit(1)

    elif RUN_MODE == "incremental":
        # -------------------------------------------------------------------
        # INCREMENTAL mode: sync recent mail (scheduled every 30 minutes).
        # Queries the last INCREMENTAL_LOOKBACK_DAYS days; the
        # aidrive-archived label prevents re-uploading already-synced mail.
        # -------------------------------------------------------------------
        start_str, end_str = _incremental_window()
        log(
            f"MODE: incremental — syncing new mail "
            f"(lookback {INCREMENTAL_LOOKBACK_DAYS} day(s): "
            f"{start_str} → {end_str})"
        )
        log(f"Folder: {AIDRIVE_FOLDER}. Cap: {MAX_EMAILS}")
        service = get_gmail_service()
        label_id = ensure_processed_label(service)
        log(f"Gmail label '{PROCESSED_LABEL}' id: {label_id}")

        s, f, c = process_window(service, label_id, start_str, end_str)
        log(
            f"\nIncremental sync complete. "
            f"Successes: {s}, failures: {f}, candidates: {c}"
        )
        if f > 0:
            sys.exit(1)

    else:
        # -------------------------------------------------------------------
        # CUSTOM mode: use explicit START_DATE and END_DATE (original behavior).
        # -------------------------------------------------------------------
        if not START_DATE or not END_DATE:
            log(
                "ERROR: RUN_MODE is not set. "
                "Provide START_DATE and END_DATE for a custom range, "
                "or set RUN_MODE=historical or RUN_MODE=incremental."
            )
            sys.exit(1)

        log(
            f"MODE: custom range — {START_DATE} → {END_DATE}. "
            f"Folder: {AIDRIVE_FOLDER}. Cap: {MAX_EMAILS}"
        )
        service = get_gmail_service()
        label_id = ensure_processed_label(service)
        log(f"Gmail label '{PROCESSED_LABEL}' id: {label_id}")

        s, f, c = process_window(service, label_id, START_DATE, END_DATE)
        log(
            f"\nCustom-range run complete. "
            f"Successes: {s}, failures: {f}, candidates: {c}"
        )
        if f > 0:
            sys.exit(1)


if __name__ == "__main__":
    main()
