"""
email_backfill.py

Pulls emails from Gmail and uploads each one as a .txt file (raw RFC 822
bytes) to AI Drive via the AI Drive API (signed-URL upload flow). The
.txt extension is used because AI Drive's signed_url_upload_batch_v2
endpoint does not accept .eml.

Designed to run unattended in GitHub Actions, but also runs locally.

Idempotent: applies a Gmail label "aidrive-archived" to each successfully
uploaded message, so reruns of the same date range skip already-processed
messages — no duplicates.

ATTACHMENT HANDLING:
  Gmail's "raw" format returns the complete RFC 822 message bytes, which
  includes all MIME parts (body text, HTML alternatives, and every attachment).
  The script uploads those raw bytes as a .txt file, so attachments are
  already embedded inside the payload. No separate attachment handling is
  required.

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

  AIDRIVE_REFRESH_TOKEN        (preferred) Firebase refresh token for the
                               AI Drive account. Read from the browser
                               localStorage entry
                               ``firebase:authUser:<apiKey>:[DEFAULT]`` →
                               ``stsTokenManager.refreshToken``. When this and
                               AIDRIVE_FIREBASE_API_KEY are both set, the
                               uploader mints fresh JWTs automatically and no
                               longer depends on a manually-pasted
                               AIDRIVE_TOKEN.
  AIDRIVE_FIREBASE_API_KEY     (preferred) Firebase Web API key. Read from the
                               same localStorage entry → ``apiKey``, e.g.
                               ``AIzaSy...``.
  AIDRIVE_TOKEN                (optional fallback) AI Drive bearer JWT pasted
                               from a logged-in browser session. Used only if
                               the Firebase refresh credentials above are not
                               set. Short-lived; will be retired automatically
                               once a Firebase refresh succeeds.
  AIDRIVE_TOKEN_FILE           (optional fallback) path to a file containing
                               the AI Drive JWT. When set, the token is
                               reloaded from this file before every API
                               request, so a new JWT can be dropped in place
                               mid-run without restarting.
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
  CHECKPOINT_FILE              (optional) path to the historical-mode checkpoint
                               file. Default ".backfill_checkpoint.json". Chunks
                               recorded here are skipped on the next run, so a
                               historical backfill interrupted by an expired
                               JWT (or any other failure) resumes where it left
                               off after the token is refreshed.

USAGE:
  python email_backfill.py
"""

import base64
import json
import os
import re
import sys
import tempfile
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


AIDRIVE_TOKEN_FILE = os.environ.get("AIDRIVE_TOKEN_FILE", "").strip()
# Firebase auto-refresh credentials (preferred). When both are set, the
# uploader mints fresh AI Drive JWTs on demand via Google Identity Toolkit's
# secure-token endpoint, so long-running historical backfills never have to
# stop for a manually-pasted token.
AIDRIVE_REFRESH_TOKEN = os.environ.get("AIDRIVE_REFRESH_TOKEN", "").strip()
AIDRIVE_FIREBASE_API_KEY = os.environ.get("AIDRIVE_FIREBASE_API_KEY", "").strip()
# Optional manual fallback. Only required when Firebase refresh credentials
# are not configured — otherwise it acts as a last-resort token until the
# first successful refresh.
_AIDRIVE_TOKEN_ENV = os.environ.get("AIDRIVE_TOKEN", "").strip()
if not (AIDRIVE_REFRESH_TOKEN and AIDRIVE_FIREBASE_API_KEY) \
        and not _AIDRIVE_TOKEN_ENV \
        and not AIDRIVE_TOKEN_FILE:
    raise RuntimeError(
        "Missing AI Drive credentials: set AIDRIVE_REFRESH_TOKEN + "
        "AIDRIVE_FIREBASE_API_KEY (preferred, enables auto-refresh) or, "
        "as a fallback, AIDRIVE_TOKEN / AIDRIVE_TOKEN_FILE. "
        "Check your GitHub Actions secrets configuration."
    )
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
# Checkpoint file: stores completed (start, end) chunks so an interrupted
# historical backfill can resume from the last successful chunk.
CHECKPOINT_FILE = os.environ.get(
    "CHECKPOINT_FILE", ".backfill_checkpoint.json"
).strip() or ".backfill_checkpoint.json"


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Permanent vs transient error classification
# ---------------------------------------------------------------------------

class AIDriveAuthExpired(Exception):
    """Raised when AI Drive returns 401 / AUTH_REQUIRED / invalid token.

    This is a permanent failure for the current process: retrying the same
    expired Bearer JWT will keep failing. Callers should preserve checkpoint
    state and exit cleanly so the operator can refresh the token.
    """


class AIDrivePermanentError(Exception):
    """Raised for permanent (non-retryable) AI Drive API failures.

    Examples: HTTP 4xx other than 401/408/429 — invalid payload, validation
    errors, unsupported file types. Retrying would just produce the same
    error and waste quota.
    """


# Substrings (case-insensitive) in a response body that indicate the JWT is
# no longer accepted, even when the HTTP status isn't 401.
_AUTH_EXPIRED_MARKERS = (
    "auth_required",
    "invalid token",
    "invalid_token",
    "token expired",
    "token has expired",
    "expired token",
    "jwt expired",
    "unauthorized",
)


def _looks_like_auth_failure(status, body):
    """Return True if the response signals an expired / invalid JWT."""
    if status == 401:
        return True
    if not body:
        return False
    lower = body.lower()
    return any(marker in lower for marker in _AUTH_EXPIRED_MARKERS)


# ---------------------------------------------------------------------------
# Dynamic AI Drive token loading + Firebase auto-refresh
# ---------------------------------------------------------------------------
#
# Bearer JWTs minted from the AI Drive (Firebase) browser session expire
# after roughly an hour, which is shorter than a full historical backfill
# can take. To avoid stopping mid-run for a manually-pasted token we mint
# fresh JWTs on demand using Firebase's secure-token endpoint:
#
#   POST https://securetoken.googleapis.com/v1/token?key=<API_KEY>
#   form-encoded: grant_type=refresh_token & refresh_token=<REFRESH_TOKEN>
#
# The response carries access_token / id_token / expires_in / user_id; we
# use id_token as the AI Drive ``Authorization: Bearer …`` value and refresh
# proactively when the cached JWT is within FIREBASE_REFRESH_SKEW_SECS of
# expiry (or reactively after an AUTH_REQUIRED / 401 response).
#
# The ``AIDRIVE_TOKEN`` / ``AIDRIVE_TOKEN_FILE`` mechanisms are kept as a
# manual fallback for environments that have not (yet) been configured with
# the Firebase refresh credentials.

FIREBASE_SECURE_TOKEN_URL = "https://securetoken.googleapis.com/v1/token"
# Refresh slightly before the JWT actually expires so an in-flight request
# never lands with a token that's about to die.
FIREBASE_REFRESH_SKEW_SECS = 120
# Hard cap on consecutive Firebase-refresh attempts before giving up. Stops
# the process from busy-looping if the refresh token itself is revoked.
FIREBASE_REFRESH_MAX_ATTEMPTS = 3

# Default TTL assumed when the Firebase response omits expires_in and the
# minted JWT carries no decodable exp claim. Conservative — Firebase ID
# tokens are nominally valid for 1 hour; this leaves headroom to refresh.
FIREBASE_DEFAULT_TTL_SECS = 50 * 60
# JWT payloads are URL-safe base64 without padding; pad to a multiple of 4
# before decoding.
_BASE64_ALIGNMENT = 4

_aidrive_token_cache = {
    "value": None,        # current bearer JWT (id_token)
    "source": None,       # "firebase" | "file:…" | "env:AIDRIVE_TOKEN"
    "expires_at": None,   # epoch seconds, or None if unknown
}


def _read_token_file(path):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read().strip()
    except FileNotFoundError:
        return ""
    except OSError as e:
        log(f"WARN: could not read AIDRIVE_TOKEN_FILE={path!r}: {e}")
        return ""


def _decode_jwt_exp(token):
    """Return the JWT ``exp`` claim (epoch seconds) or ``None`` on failure.

    Used so manually-pasted tokens (without an explicit expires_in from the
    refresh endpoint) can still participate in proactive expiry detection.
    Decoding is best-effort and never validates the signature — we just need
    the unsigned ``exp`` claim for scheduling.
    """
    if not token or token.count(".") != 2:
        return None
    try:
        payload_b64 = token.split(".")[1]
        padding = "=" * (-len(payload_b64) % _BASE64_ALIGNMENT)
        raw = base64.urlsafe_b64decode(payload_b64 + padding)
        claims = json.loads(raw.decode("utf-8", errors="ignore"))
    except (ValueError, json.JSONDecodeError):
        return None
    exp = claims.get("exp")
    return int(exp) if isinstance(exp, (int, float)) else None


def is_token_expired(expires_at=None, skew=FIREBASE_REFRESH_SKEW_SECS):
    """Return True when the cached token has expired (or will, within skew).

    Treats an unknown ``expires_at`` as "expired" so the caller refreshes —
    this is the safe default when we have no idea how long a manually pasted
    token has left.
    """
    if expires_at is None:
        return True
    return time.time() + skew >= expires_at


def _redact(value, keep=4):
    """Return a short, non-sensitive marker for secret values.

    Never returns enough characters to reconstruct the secret. Used only in
    structured log lines so operators can correlate refresh events without
    leaking the JWT itself.
    """
    if not value:
        return "<empty>"
    if len(value) <= keep * 2:
        return "<redacted>"
    return f"<redacted len={len(value)}>"


def refresh_firebase_token():
    """Mint a fresh AI Drive JWT via Firebase's secure-token endpoint.

    Updates the in-memory token cache on success and returns the new
    ``id_token``. Raises :class:`AIDriveAuthExpired` if the refresh token
    is rejected (HTTP 400 ``INVALID_REFRESH_TOKEN`` /
    ``TOKEN_EXPIRED`` / ``USER_DISABLED`` etc.) so the caller can stop
    cleanly. Network / 5xx errors raise ``RuntimeError`` so the surrounding
    retry classifier can treat them as transient.
    """
    if not (AIDRIVE_REFRESH_TOKEN and AIDRIVE_FIREBASE_API_KEY):
        raise AIDriveAuthExpired(
            "Firebase refresh credentials are not configured "
            "(set AIDRIVE_REFRESH_TOKEN and AIDRIVE_FIREBASE_API_KEY)."
        )
    last_err = None
    for attempt in range(FIREBASE_REFRESH_MAX_ATTEMPTS):
        try:
            r = requests.post(
                FIREBASE_SECURE_TOKEN_URL,
                params={"key": AIDRIVE_FIREBASE_API_KEY},
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": AIDRIVE_REFRESH_TOKEN,
                },
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                },
                timeout=REQUEST_TIMEOUT_SECS,
            )
        except _TRANSIENT_NETWORK_EXCEPTIONS as e:
            last_err = f"network error: {type(e).__name__}: {e}"
            log(
                "event=firebase_refresh attempt=%d classification=transient "
                "reason=network_error" % (attempt + 1)
            )
            time.sleep(RETRY_DELAY_SECS)
            continue
        except requests.RequestException as e:
            raise RuntimeError(
                f"Firebase refresh transport error: {type(e).__name__}: {e}"
            ) from e

        if r.status_code == 200:
            try:
                payload = r.json()
            except ValueError as e:
                raise RuntimeError(
                    f"Firebase refresh returned non-JSON body: {e}"
                ) from e
            id_token = (payload.get("id_token") or "").strip()
            access_token = (payload.get("access_token") or "").strip()
            user_id = payload.get("user_id") or ""
            try:
                expires_in = int(payload.get("expires_in") or 0)
            except (TypeError, ValueError):
                expires_in = 0
            # Prefer id_token (Firebase ID JWT) for AI Drive's
            # ``Authorization: Bearer …`` header. Fall back to access_token
            # if the response shape ever differs; both are JWTs minted by
            # the same secure-token service.
            new_token = id_token or access_token
            if not new_token:
                raise AIDriveAuthExpired(
                    "Firebase refresh succeeded but returned no id_token / "
                    "access_token — check AIDRIVE_FIREBASE_API_KEY."
                )
            jwt_exp = _decode_jwt_exp(new_token)
            now = time.time()
            if expires_in > 0:
                expires_at = now + expires_in
            elif jwt_exp:
                expires_at = float(jwt_exp)
            else:
                # Conservative default when neither expires_in nor an
                # exp claim is available — still refresh proactively.
                expires_at = now + FIREBASE_DEFAULT_TTL_SECS
            old = _aidrive_token_cache.get("value")
            _aidrive_token_cache.update(
                {
                    "value": new_token,
                    "source": "firebase",
                    "expires_at": expires_at,
                }
            )
            ttl = max(0, int(expires_at - now))
            log(
                "event=firebase_refresh status=ok user_id=%s ttl_secs=%d "
                "rotated=%s token=%s"
                % (
                    user_id or "<unknown>",
                    ttl,
                    "true" if old and old != new_token else "false",
                    _redact(new_token),
                )
            )
            return new_token

        # Non-200 — classify.
        body = r.text or ""
        last_err = f"HTTP {r.status_code} {body[:200]}"
        body_lower = body.lower()
        permanent_markers = (
            "invalid_refresh_token",
            "token_expired",
            "user_disabled",
            "user_not_found",
            "invalid_grant",
        )
        is_permanent = (
            r.status_code in (400, 401, 403)
            and any(m in body_lower for m in permanent_markers)
        )
        if is_permanent:
            log(
                f"event=firebase_refresh status=rejected "
                f"http={r.status_code} reason=permanent"
            )
            raise AIDriveAuthExpired(
                f"Firebase refresh token rejected ({last_err}). "
                "Re-extract AIDRIVE_REFRESH_TOKEN from the browser "
                "(localStorage → firebase:authUser:<apiKey>:[DEFAULT] → "
                "stsTokenManager.refreshToken)."
            )
        if r.status_code in _TRANSIENT_RETRY_STATUSES:
            log(
                f"event=firebase_refresh attempt={attempt + 1} "
                f"classification=transient http={r.status_code}"
            )
            time.sleep(RETRY_DELAY_SECS)
            continue
        # Other 4xx: surface as a hard runtime error so the retry classifier
        # in the caller doesn't loop on a misconfiguration (e.g. wrong
        # API key shape).
        log(
            f"event=firebase_refresh status=failed http={r.status_code} "
            f"classification=permanent"
        )
        raise RuntimeError(f"Firebase refresh failed: {last_err}")
    raise RuntimeError(
        f"Firebase refresh failed after {FIREBASE_REFRESH_MAX_ATTEMPTS} "
        f"attempts: {last_err}"
    )


def _load_fallback_token():
    """Return ``(token, source)`` from AIDRIVE_TOKEN_FILE / AIDRIVE_TOKEN.

    Both sources are re-read on every call (rather than reusing the value
    captured at import time in ``_AIDRIVE_TOKEN_ENV``) so an operator can
    swap in a refreshed JWT mid-run without restarting the process.
    """
    if AIDRIVE_TOKEN_FILE:
        token = _read_token_file(AIDRIVE_TOKEN_FILE)
        if token:
            return token, f"file:{AIDRIVE_TOKEN_FILE}"
    token = os.environ.get("AIDRIVE_TOKEN", "").strip()
    if token:
        return token, "env:AIDRIVE_TOKEN"
    return "", None


def get_aidrive_token():
    """Return a usable AI Drive bearer JWT.

    Resolution order:

    1. Cached Firebase-refreshed JWT, if still valid (proactive expiry).
    2. Mint a fresh JWT via :func:`refresh_firebase_token` when refresh
       credentials are configured.
    3. Cached non-Firebase token if it has not changed.
    4. ``AIDRIVE_TOKEN_FILE`` then ``AIDRIVE_TOKEN`` (manual fallback).

    The token is held in memory only — it is never written to disk and
    never logged in full. A structured ``event=token_*`` line is emitted
    whenever the resolved token changes so refresh activity is traceable.
    """
    cached_value = _aidrive_token_cache.get("value")
    cached_source = _aidrive_token_cache.get("source")
    cached_exp = _aidrive_token_cache.get("expires_at")

    # 1. Reuse cached Firebase-minted token while it's still good.
    if (
        cached_value
        and cached_source == "firebase"
        and not is_token_expired(cached_exp)
    ):
        return cached_value

    # 2. Prefer Firebase auto-refresh when configured.
    if AIDRIVE_REFRESH_TOKEN and AIDRIVE_FIREBASE_API_KEY:
        if cached_value and cached_source == "firebase":
            log(
                "event=token_refresh_due reason=expired_or_near_expiry "
                "source=firebase"
            )
        return refresh_firebase_token()

    # 3 + 4. Manual fallback — keep prior behaviour.
    token, source = _load_fallback_token()
    if not token:
        raise AIDriveAuthExpired(
            "AI Drive JWT is not available — set AIDRIVE_REFRESH_TOKEN + "
            "AIDRIVE_FIREBASE_API_KEY (preferred) or AIDRIVE_TOKEN / "
            "AIDRIVE_TOKEN_FILE."
        )
    if cached_value != token:
        if cached_value is None:
            log(f"event=token_loaded source={source}")
        else:
            log(
                f"event=token_reloaded source={source} "
                f"reason=value_changed"
            )
        # Best-effort expiry from the JWT itself so we still know when to
        # treat the manual token as stale.
        exp_claim = _decode_jwt_exp(token)
        _aidrive_token_cache.update(
            {
                "value": token,
                "source": source,
                "expires_at": float(exp_claim) if exp_claim else None,
            }
        )
    return token


def get_valid_aidrive_token():
    """Return a token guaranteed not to be near expiry.

    Convenience wrapper that proactively refreshes the cached token when
    it is within the refresh skew window. Equivalent to calling
    :func:`get_aidrive_token` plus an explicit expiry check, exposed as a
    distinct helper so callers (and tests) can document the proactive
    intent explicitly.
    """
    cached_value = _aidrive_token_cache.get("value")
    cached_exp = _aidrive_token_cache.get("expires_at")
    if cached_value and not is_token_expired(cached_exp):
        return cached_value
    return get_aidrive_token()


def force_reload_aidrive_token():
    """Drop the cached token and re-resolve.

    Returns ``(old_token, new_token)``. Used after an AI Drive AUTH_REQUIRED
    response: with Firebase credentials this triggers a fresh refresh;
    without them it re-reads ``AIDRIVE_TOKEN_FILE`` / ``AIDRIVE_TOKEN`` so
    an operator can swap in a new manually-pasted JWT mid-run.
    """
    old_token = _aidrive_token_cache.get("value")
    _aidrive_token_cache.update({"value": None, "expires_at": None})
    try:
        new_token = get_aidrive_token()
    except AIDriveAuthExpired:
        new_token = None
    return old_token, new_token


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

# Extensions AI Drive accepts. ``.eml`` is NOT in this list — AI Drive's
# ``signed_url_upload_batch_v2`` enum rejects it (HTTP 422). RFC 822 message
# bodies are uploaded with a ``.txt`` extension instead (see
# ``_MIME_TO_EXT`` below). Keep entries lowercase with the leading dot.
ALLOWED_EXTENSIONS = frozenset({
    ".pdf", ".csv", ".xlsx", ".xls", ".docx", ".doc",
    ".pptx", ".ppt", ".txt", ".md",
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
    # AI Drive does not accept .eml; archive RFC 822 messages as .txt.
    "message/rfc822": ".txt",
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
    """Return AI Drive request headers with a guaranteed-valid bearer JWT.

    Uses :func:`get_valid_aidrive_token` so a near-expiry token is refreshed
    proactively (via Firebase when configured) before the request goes out,
    rather than waiting for an AUTH_REQUIRED response.
    """
    return {
        "Authorization": f"Bearer {get_valid_aidrive_token()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


# Status codes worth retrying for transient AI Drive failures.
_TRANSIENT_RETRY_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})

# requests exception types that always represent transient transport issues
# (connection resets, DNS hiccups, read/connect timeouts).
_TRANSIENT_NETWORK_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


def _post_with_retries(url, json_body, label):
    """POST JSON to ``url`` with smart retry classification.

    Retries ONLY transient failures: HTTP 408/429/5xx, connection resets and
    timeouts. Permanent failures fail fast:

    * Expired / invalid JWT (HTTP 401, AUTH_REQUIRED, "invalid token") →
      one token reload is attempted; if the token is unchanged or the next
      attempt still fails the same way, :class:`AIDriveAuthExpired` is
      raised so the caller can preserve checkpoint state and exit.
    * Other 4xx (validation errors, unsupported file types, malformed
      payloads) → :class:`AIDrivePermanentError` is raised immediately.

    Returns parsed JSON on success.
    """
    last_err = None
    auth_reload_attempted = False
    for attempt in range(MAX_RETRIES):
        status = None
        body = ""
        try:
            r = requests.post(
                url,
                headers=aidrive_headers(),
                json=json_body,
                timeout=REQUEST_TIMEOUT_SECS,
            )
            status = r.status_code
            body = r.text or ""
            if status == 200:
                return r.json()
            last_err = f"HTTP {status} {body[:200]}"
        except _TRANSIENT_NETWORK_EXCEPTIONS as e:
            last_err = f"network error: {type(e).__name__}: {e}"
            log(
                f"{label} attempt {attempt + 1} classification=transient "
                f"reason=network_error err={last_err}"
            )
            if attempt == MAX_RETRIES - 1:
                break
            time.sleep(RETRY_DELAY_SECS)
            continue
        except requests.RequestException as e:
            # Other requests errors (e.g. invalid URL, SSL handshake) are
            # not transient — surface immediately as a permanent error.
            raise AIDrivePermanentError(
                f"{label} permanent transport error: {type(e).__name__}: {e}"
            ) from e

        # Classify the HTTP response.
        if _looks_like_auth_failure(status, body):
            log(
                f"{label} attempt {attempt + 1} classification=auth_failure "
                f"status={status}"
            )
            if not auth_reload_attempted:
                auth_reload_attempted = True
                try:
                    old_tok, new_tok = force_reload_aidrive_token()
                except AIDriveAuthExpired:
                    old_tok, new_tok = _aidrive_token_cache.get("value"), None
                if new_tok and new_tok != old_tok:
                    log(
                        f"event=auth_recovery action=token_refreshed "
                        f"label={label} source={_aidrive_token_cache.get('source')} "
                        "— retrying once with refreshed JWT"
                    )
                    continue
                log(
                    "event=auth_failure action=give_up "
                    "reason=refresh_did_not_yield_new_token "
                    f"label={label}"
                )
            raise AIDriveAuthExpired(
                f"AI Drive JWT expired — refresh required ({label}: HTTP {status})"
            )

        if status in _TRANSIENT_RETRY_STATUSES:
            log(
                f"{label} attempt {attempt + 1} classification=transient "
                f"status={status} err={last_err}"
            )
            if attempt == MAX_RETRIES - 1:
                break
            time.sleep(RETRY_DELAY_SECS)
            continue

        # Any other 4xx is a permanent client error — invalid payload,
        # validation error, unsupported file type, etc. Do not retry.
        if status is not None and 400 <= status < 500:
            log(
                f"{label} attempt {attempt + 1} classification=permanent "
                f"status={status} err={last_err}"
            )
            raise AIDrivePermanentError(
                f"{label} permanent failure: {last_err}"
            )

        # Unknown status — treat as transient to be safe.
        log(
            f"{label} attempt {attempt + 1} classification=transient "
            f"status={status} err={last_err}"
        )
        if attempt == MAX_RETRIES - 1:
            break
        time.sleep(RETRY_DELAY_SECS)

    raise RuntimeError(f"{label} failed after {MAX_RETRIES} retries: {last_err}")


def request_signed_urls(file_batch):
    """
    file_batch: list of dicts with keys: name, path, size, file_type
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
                    # AI Drive's enum requires the leading dot
                    # (e.g. ".pdf", ".txt"). Normalize defensively in case
                    # an upstream caller passes the bare extension.
                    "file_type": (
                        f["file_type"]
                        if str(f["file_type"]).startswith(".")
                        else f".{f['file_type']}"
                    ),
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
    """PUT the bytes to Google Cloud Storage using the signed URL.

    Retries only transient failures (HTTP 408/429/5xx, connection resets,
    timeouts). Permanent 4xx responses fail fast — re-PUTting a malformed
    request to the same signed URL will keep returning the same error.
    """
    url = signed_url_obj["url"]
    extra_headers = signed_url_obj.get("headers") or {}
    # Default Content-Type matches the file_type the signed URL was minted
    # for (.txt). Any Content-Type the signed-URL response specifies wins.
    headers = {"Content-Type": "text/plain"}
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
            if r.status_code in _TRANSIENT_RETRY_STATUSES:
                log(
                    f"GCS PUT attempt {attempt + 1} classification=transient "
                    f"status={r.status_code}"
                )
            elif 400 <= r.status_code < 500:
                log(
                    f"GCS PUT attempt {attempt + 1} classification=permanent "
                    f"status={r.status_code} err={last_err}"
                )
                return False
            else:
                log(
                    f"GCS PUT attempt {attempt + 1} classification=transient "
                    f"status={r.status_code}"
                )
        except _TRANSIENT_NETWORK_EXCEPTIONS as e:
            last_err = f"network error: {type(e).__name__}: {e}"
            log(
                f"GCS PUT attempt {attempt + 1} classification=transient "
                f"reason=network_error err={last_err}"
            )
        except requests.RequestException as e:
            log(
                f"GCS PUT attempt {attempt + 1} classification=permanent "
                f"err={type(e).__name__}: {e}"
            )
            return False
        time.sleep(RETRY_DELAY_SECS)
    log(f"GCS PUT failed after {MAX_RETRIES} retries: {last_err}")
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
    except AIDriveAuthExpired:
        # Bubble up so the run-level handler can preserve checkpoint state
        # and exit cleanly. The upload itself already succeeded; the next
        # run with a refreshed JWT will re-register through the normal
        # idempotent flow.
        raise
    except (AIDrivePermanentError, RuntimeError) as e:
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
# Resumable-checkpoint helpers (historical mode)
# ---------------------------------------------------------------------------
#
# The checkpoint file records every (start, end) chunk that has finished
# successfully. On the next run those chunks are skipped, so an interrupted
# historical backfill (expired JWT, network outage, manual cancellation)
# resumes from where it left off instead of restarting all 12 months from
# scratch. The file is intentionally tiny (a JSON list of strings) and is
# written atomically so a crash mid-write cannot corrupt the state.

CHECKPOINT_VERSION = 1


def _chunk_key(start_str, end_str):
    return f"{start_str}|{end_str}"


def load_checkpoint(path=None):
    """Load completed-chunk keys from the checkpoint file.

    Returns a set. Missing / unreadable / malformed files are treated as an
    empty checkpoint so a corrupt file never blocks a re-run.
    """
    path = path or CHECKPOINT_FILE
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        log(f"event=checkpoint_restore status=empty path={path}")
        return set()
    except (OSError, json.JSONDecodeError) as e:
        log(
            f"event=checkpoint_restore status=ignored_corrupt path={path} "
            f"err={type(e).__name__}: {e}"
        )
        return set()
    if not isinstance(data, dict):
        log(f"event=checkpoint_restore status=ignored_bad_format path={path}")
        return set()
    completed = data.get("completed_chunks") or []
    if not isinstance(completed, list):
        log(f"event=checkpoint_restore status=ignored_bad_format path={path}")
        return set()
    keys = {str(k) for k in completed}
    log(
        f"event=checkpoint_restore status=loaded path={path} "
        f"completed_chunks={len(keys)}"
    )
    return keys


def save_checkpoint(completed_keys, path=None):
    """Atomically persist the set of completed chunk keys.

    Writes to a sibling temp file and renames into place so a crash or
    SIGKILL mid-write cannot leave a partially-written (and unparseable)
    checkpoint file behind.
    """
    path = path or CHECKPOINT_FILE
    payload = {
        "version": CHECKPOINT_VERSION,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "completed_chunks": sorted(completed_keys),
    }
    directory = os.path.dirname(os.path.abspath(path))
    try:
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=".checkpoint-", suffix=".json.tmp", dir=directory
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, path)
        except Exception:
            # Best-effort cleanup of the temp file.
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        log(
            f"event=checkpoint_save path={path} "
            f"completed_chunks={len(completed_keys)}"
        )
    except OSError as e:
        log(
            f"event=checkpoint_save status=failed path={path} "
            f"err={type(e).__name__}: {e}"
        )


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def process_window(service, label_id, start_str, end_str):
    """Upload all unlabelled emails in [start_str, end_str) to AI Drive.

    Returns (successes, failures, candidates) counts.
    Emails are fetched as raw RFC 822 bytes and uploaded as ``.txt`` files
    (AI Drive does not accept ``.eml``). The raw bytes still include the
    full message body AND all attachments embedded in the MIME structure —
    no separate attachment handling is needed.
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
                "file_type": item["drive_object"]["file_type"],
            }
            for item in batch
        ]

        try:
            signed_responses = request_signed_urls(batch_meta)
        except AIDriveAuthExpired:
            # Don't drop the batch silently — propagate so the run-level
            # handler preserves checkpoint state and exits cleanly.
            raise
        except AIDrivePermanentError as e:
            log(
                f"  Batch signed-URL request rejected as permanent failure: "
                f"{e}. Skipping batch (no retry)."
            )
            failures += len(batch)
            batch.clear()
            return
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

        # Emails are uploaded as RFC 822 bodies but AI Drive does not accept
        # ``.eml``; archive them as ``.txt`` instead. ``mime_type`` is still
        # recorded for logs and so ``normalize_extension`` has a sensible
        # fallback (``message/rfc822`` → ``.txt`` via ``_MIME_TO_EXT``).
        mime_type = "message/rfc822"
        original_filename = (
            f"{subject_raw}.txt" if subject_raw else f"{msg_id[:8]}.txt"
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


def _run_historical(service, label_id):
    """Historical mode with resumable checkpoints."""
    log(f"Folder: {AIDRIVE_FOLDER}. Per-chunk cap: {MAX_EMAILS}")

    chunks = list(_monthly_chunks(months_back=12))
    log(f"Total chunks to process: {len(chunks)}")

    completed = load_checkpoint()
    skipped_chunks = 0
    for start_str, end_str in chunks:
        if _chunk_key(start_str, end_str) in completed:
            skipped_chunks += 1
    if skipped_chunks:
        log(
            f"event=checkpoint_skip count={skipped_chunks} "
            f"reason=already_completed_in_previous_run"
        )

    total_successes, total_failures, total_candidates = 0, 0, 0
    for idx, (start_str, end_str) in enumerate(chunks, start=1):
        key = _chunk_key(start_str, end_str)
        if key in completed:
            log(
                f"\n[Chunk {idx}/{len(chunks)}] {start_str} → {end_str} "
                f"event=skipped reason=checkpoint"
            )
            continue
        log(f"\n[Chunk {idx}/{len(chunks)}] {start_str} → {end_str}")
        s, f, c = process_window(service, label_id, start_str, end_str)
        total_successes += s
        total_failures += f
        total_candidates += c
        log(
            f"[Chunk {idx}/{len(chunks)}] done — "
            f"successes: {s}, failures: {f}, candidates: {c}"
        )
        # Persist progress immediately so a later failure cannot lose this chunk.
        completed.add(key)
        save_checkpoint(completed)

    log(
        f"\nHistorical backfill complete. "
        f"Total successes: {total_successes}, "
        f"failures: {total_failures}, "
        f"candidates: {total_candidates}"
    )
    return total_failures


def main():
    log("=" * 60)

    try:
        if RUN_MODE == "historical":
            log("MODE: historical — processing last 12 months month by month")
            service = get_gmail_service()
            label_id = ensure_processed_label(service)
            log(f"Gmail label '{PROCESSED_LABEL}' id: {label_id}")
            failures = _run_historical(service, label_id)
            if failures > 0:
                sys.exit(1)

        elif RUN_MODE == "incremental":
            # ---------------------------------------------------------------
            # INCREMENTAL mode: sync recent mail (scheduled every 30 minutes).
            # ---------------------------------------------------------------
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
            # ---------------------------------------------------------------
            # CUSTOM mode: use explicit START_DATE and END_DATE.
            # ---------------------------------------------------------------
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

    except AIDriveAuthExpired as e:
        # Stop the current batch cleanly. Any historical chunks already
        # finished are preserved in the checkpoint file (saved per-chunk),
        # so a re-run with a refreshed JWT resumes where this run stopped.
        log("=" * 60)
        log("AI Drive JWT expired — refresh required")
        log(f"event=auth_failure detail={e}")
        log(
            "Refresh the Bearer JWT (browser DevTools → copy from a logged-in "
            "AI Drive request) and update AIDRIVE_TOKEN (or the file pointed "
            "to by AIDRIVE_TOKEN_FILE), then re-run. Historical progress is "
            f"preserved in {CHECKPOINT_FILE}."
        )
        # Use a distinct exit code so callers / CI can react specifically to
        # an auth-expiry termination vs ordinary failures.
        sys.exit(2)


if __name__ == "__main__":
    main()
