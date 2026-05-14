"""
email_backfill.py

Pulls emails from Gmail and uploads each one as a .eml file to AI Drive
via the AI Drive API (signed-URL upload flow).

Designed to run in GitHub Actions (no local install needed) but also runs
locally. Idempotent: applies a Gmail label "aidrive-archived" to each
processed email, so reruns skip already-uploaded messages.

ENVIRONMENT VARIABLES (set as GitHub Secrets, or local .env):

  AIDRIVE_API_KEY          your AI Drive API key
  AIDRIVE_FOLDER           destination folder, e.g. "04 - EMAIL ARCHIVE"
  GMAIL_CLIENT_ID          OAuth client id from Google Cloud Console
  GMAIL_CLIENT_SECRET      OAuth client secret
  GMAIL_REFRESH_TOKEN      refresh token obtained via get_gmail_token.py
  START_DATE               YYYY/MM/DD, e.g. 2025/05/14
  END_DATE                 YYYY/MM/DD, e.g. 2025/06/14
  MAX_EMAILS               (optional) cap per run, default 5000

USAGE:
  python email_backfill.py
"""

import base64
import os
import re
import sys
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

AIDRIVE_BASE = "https://ai-drive-api-prod-qvg2narjsa-uc.a.run.app"
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]
PROCESSED_LABEL = "aidrive-archived"
BATCH_SIZE = 25
MAX_RETRIES = 3
RETRY_DELAY_SECS = 5

AIDRIVE_API_KEY = os.environ["AIDRIVE_API_KEY"]
AIDRIVE_FOLDER = os.environ.get("AIDRIVE_FOLDER", "04 - EMAIL ARCHIVE")
GMAIL_CLIENT_ID = os.environ["GMAIL_CLIENT_ID"]
GMAIL_CLIENT_SECRET = os.environ["GMAIL_CLIENT_SECRET"]
GMAIL_REFRESH_TOKEN = os.environ["GMAIL_REFRESH_TOKEN"]
START_DATE = os.environ["START_DATE"]
END_DATE = os.environ["END_DATE"]
MAX_EMAILS = int(os.environ.get("MAX_EMAILS", "5000"))


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


def ensure_processed_label(service):
    labels = service.users().labels().list(userId="me").execute().get("labels", [])
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


def sanitize_for_filename(text, max_len=80):
    if not text:
        return "no-subject"
    cleaned = re.sub(r'[\/\\:*?"<>|\r\n\t]', "_", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:max_len] or "no-subject"


def parse_date_safe(date_header):
    try:
        return parsedate_to_datetime(date_header)
    except Exception:
        return datetime.now(timezone.utc)


def list_message_ids(service, query):
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
    msg = service.users().messages().get(
        userId="me",
        id=msg_id,
        format="raw",
    ).execute()
    raw_bytes = base64.urlsafe_b64decode(msg["raw"])

    headers_parts = raw_bytes.split(b"\r\n\r\n", 1)
    headers_text = headers_parts[0].decode("utf-8", errors="ignore")
    headers = {}
    for line in headers_text.splitlines():
        if ":" in line and not line.startswith(" "):
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()

    parsed_date = parse_date_safe(headers.get("date", ""))
    subject_safe = sanitize_for_filename(headers.get("subject", ""))
    from_raw = headers.get("from", "")
    from_name = from_raw.split("<")[0].strip() or from_raw.strip()
    from_safe = sanitize_for_filename(from_name, 40)
    return raw_bytes, parsed_date, subject_safe, from_safe


def build_filename(parsed_date, subject_safe, from_safe, msg_id):
    return (
        f"{parsed_date.strftime('%Y-%m-%d_%H%M')}_"
        f"{from_safe}_{subject_safe}_{msg_id[:8]}.eml"
    )


def build_folder_path(parsed_date):
    return f"{AIDRIVE_FOLDER}/{parsed_date.strftime('%Y-%m')}"


def aidrive_headers():
    return {
        "Authorization": f"Bearer {AIDRIVE_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def request_signed_urls(file_batch):
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
    for attempt in range(MAX_RETRIES):
        r = requests.post(
            f"{AIDRIVE_BASE}/signed_url_upload_batch_v2",
            headers=aidrive_headers(),
            json=body,
            timeout=60,
        )
        if r.status_code == 200:
            return r.json()
        log(
            f"signed_url_upload_batch_v2 attempt {attempt+1} failed: "
            f"{r.status_code} {r.text[:200]}"
        )
        time.sleep(RETRY_DELAY_SECS)
    raise RuntimeError("Failed to get signed URLs after retries")


def upload_to_gcs(signed_url_obj, raw_bytes):
    url = signed_url_obj["url"]
    extra_headers = signed_url_obj.get("headers") or {}
    headers = {"Content-Type": "message/rfc822"}
    headers.update(extra_headers)
    for attempt in range(MAX_RETRIES):
        r = requests.put(url, data=raw_bytes, headers=headers, timeout=120)
        if r.status_code in (200, 201):
            return True
        log(f"GCS PUT attempt {attempt+1} failed: {r.status_code} {r.text[:200]}")
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
    for attempt in range(MAX_RETRIES):
        r = requests.post(
            f"{AIDRIVE_BASE}/file_upload_status_v2",
            headers=aidrive_headers(),
            json=body,
            timeout=60,
        )
        if r.status_code == 200:
            return True
        log(
            f"file_upload_status_v2 attempt {attempt+1} failed: "
            f"{r.status_code} {r.text[:200]}"
        )
        time.sleep(RETRY_DELAY_SECS)
    return False


def label_message_processed(service, msg_id, label_id):
    service.users().messages().modify(
        userId="me",
        id=msg_id,
        body={"addLabelIds": [label_id]},
    ).execute()


def main():
    log(
        f"Starting backfill. Range: {START_DATE} to {END_DATE}. "
        f"Folder: {AIDRIVE_FOLDER}. Cap: {MAX_EMAILS}"
    )
    service = get_gmail_service()
    label_id = ensure_processed_label(service)
    log(f"Gmail label '{PROCESSED_LABEL}' id: {label_id}")

    query = f"after:{START_DATE} before:{END_DATE} -label:{PROCESSED_LABEL}"
    log(f"Gmail query: {query}")

    msg_ids = list(list_message_ids(service, query))
    log(f"Found {len(msg_ids)} candidate emails")

    successes, failures = 0, 0
    batch = []

    def flush_batch():
        nonlocal successes, failures
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
            log(f"Batch signed-URL request failed: {e}. Skipping batch.")
            failures += len(batch)
            batch.clear()
            return

        for item, signed_resp in zip(batch, signed_responses):
            if signed_resp.get("error") or not signed_resp.get("signed_url"):
                log(f"  Skip {item['drive_object']['name']}: {signed_resp.get('error')}")
                failures += 1
                continue

            signed_obj = signed_resp["signed_url"]
            t0 = time.time()
            ok = upload_to_gcs(signed_obj, item["raw_bytes"])
            duration = time.time() - t0
            if not ok:
                failures += 1
                continue

            registered = register_upload(
                drive_object=signed_resp["drive_object"],
                signed_url_str=signed_obj["url"],
                size_mb=item["size_mb"],
                success=True,
                duration=duration,
            )
            if not registered:
                failures += 1
                continue

            try:
                label_message_processed(service, item["msg_id"], label_id)
            except Exception as e:
                log(f"  Warning: could not label {item['msg_id']}: {e}")

            successes += 1
            log(f"  OK ({successes}): {item['drive_object']['name']}")

        batch.clear()

    for i, msg_id in enumerate(msg_ids, start=1):
        try:
            raw_bytes, parsed_date, subject_safe, from_safe = fetch_raw_email(
                service, msg_id
            )
        except Exception as e:
            log(f"  Error fetching {msg_id}: {e}")
            failures += 1
            continue

        filename = build_filename(parsed_date, subject_safe, from_safe, msg_id)
        folder_path = build_folder_path(parsed_date)
        size_bytes = len(raw_bytes)
        size_mb = size_bytes / (1024 * 1024)

        drive_object = {
            "name": filename,
            "path": folder_path,
            "isFile": True,
            "file_type": "eml",
        }

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
                f"Progress: {i}/{len(msg_ids)} "
                f"(successes: {successes}, failures: {failures})"
            )

    flush_batch()
    log(
        f"Done. Successes: {successes}, Failures: {failures}, "
        f"Total candidates: {len(msg_ids)}"
    )

    if failures > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
