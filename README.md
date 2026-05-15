# email_backfill.py

Automatically syncs Gmail messages into [My AI Drive](https://myaidrive.com) as
individual `.txt` files (raw RFC 822 message bodies) using the AI Drive
signed-URL upload API. AI Drive does not accept `.eml`, so the raw email
bytes are archived under a `.txt` extension.
individual `.txt` files (with attachments uploaded separately when their format
is supported) using the AI Drive signed-URL upload API.

The script runs unattended in **GitHub Actions** on a **30-minute schedule**
and can also be triggered manually for a full 12-month historical catch-up.
It is **idempotent**: each successfully uploaded message is tagged in Gmail with
the label `aidrive-archived`, so reruns of any date range are always safe — no
duplicate uploads.

## Repository contents

| File | Purpose |
| --- | --- |
| `email_backfill.py` | Main job — Gmail → AI Drive. Supports historical, incremental, and custom date-range modes. |
| `get_gmail_token.py` | One-time local helper to mint a Gmail OAuth refresh token. |
| `.github/workflows/email_backfill.yml` | Scheduled (every 30 min) + manual workflow. |
| `requirements.txt` | Pinned Python dependencies. |

> Only the workflow under `.github/workflows/` is loaded by GitHub Actions.

## One-time setup

### 1. Create a Google Cloud OAuth client

1. In the [Google Cloud Console](https://console.cloud.google.com/), create a
   project, enable the **Gmail API**, and create an OAuth 2.0 Client ID of
   type **Desktop app**.
2. Download the client JSON and save it locally as `credentials.json`
   (this file is gitignored — never commit it).

### 2. Mint a refresh token (run locally, once)

```bash
pip install google-auth-oauthlib google-api-python-client
python get_gmail_token.py
```

This opens a browser window so you can grant the `gmail.modify` scope. On
success it prints `GMAIL_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, and
`GMAIL_REFRESH_TOKEN`. Delete `credentials.json` afterwards.

### 3. Configure GitHub repository secrets

Add the following under **Settings → Secrets and variables → Actions**.
**Preferred** is the Firebase auto-refresh pair, which lets long-running
backfills mint fresh AI Drive JWTs on demand and never stop for a
manually-pasted token:

| Secret | Required | Description |
| --- | --- | --- |
| `AIDRIVE_REFRESH_TOKEN` | preferred | Firebase refresh token from `stsTokenManager.refreshToken` (see [Firebase auto-refresh](#ai-drive-jwt-expiration--firebase-auto-refresh)). Long-lived. |
| `AIDRIVE_FIREBASE_API_KEY` | preferred | Firebase Web API key (`apiKey` field, e.g. `AIzaSy…`) from the same browser localStorage entry. |
| `AIDRIVE_TOKEN` | fallback | Short-lived bearer JWT pasted from the AI Drive browser session. Only needed when the two values above are not set. |
| `GMAIL_CLIENT_ID` | yes | OAuth client id from step 2. |
| `GOOGLE_OAUTH_CLIENT_SECRET` | yes | OAuth client secret from step 2. |
| `GMAIL_REFRESH_TOKEN` | yes | Refresh token from step 2. |

> **Never commit any of these values.** `.env`, token files, and the
> historical-mode checkpoint file are all listed in `.gitignore`.

## Operating modes

### Automatic 30-minute sync (incremental)

Once the workflow is in place, GitHub Actions **automatically runs every
30 minutes** without any manual intervention. Each scheduled run uses
**incremental mode**: it queries the last 2 calendar days for any mail not yet
labeled `aidrive-archived` and uploads it. The 2-day window is intentionally
wider than 30 minutes so that no mail is missed across midnight boundaries or
transient API outages.

You don't need to do anything to keep this running — it starts automatically
after the workflow file is committed.

### Historical 12-month backfill (historical)

To import everything from the last 12 months in one go:

1. Open **Actions** → **Email Backfill** → **Run workflow**.
2. Set **Run mode** to `historical` (this is the default).
3. Click **Run workflow**.

The job works through the last 12 calendar months one month at a time and logs
each chunk. Because of the `aidrive-archived` label, you can safely re-trigger
this at any time — already-processed messages are skipped.

### Custom date range (custom)

For a one-off import of a specific period:

1. Open **Actions** → **Email Backfill** → **Run workflow**.
2. Set **Run mode** to `custom`.
3. Fill in **Start date** and **End date** (format `YYYY/MM/DD`).
4. Click **Run workflow**.

| Input | Required | Default | Notes |
| --- | --- | --- | --- |
| `run_mode` | no | `historical` | `historical` \| `incremental` \| `custom` |
| `start_date` | only in custom mode | — | Inclusive lower bound. Format `YYYY/MM/DD`. |
| `end_date` | only in custom mode | — | Exclusive upper bound. Format `YYYY/MM/DD`. |
| `max_emails` | no | `2000` | Hard cap on messages processed per chunk. |
| `aidrive_folder` | no | `04 - EMAIL ARCHIVE` | Top-level AI Drive folder. Files land in `<folder>/YYYY-MM/`. |

## Attachments

**Attachments are automatically included.** Gmail's `raw` format returns the
complete [RFC 822](https://www.rfc-editor.org/rfc/rfc822) message bytes, which
contain the full MIME structure — body text, HTML alternative, and every
attachment — all in a single binary blob. The script uploads this blob as a
`.txt` file (AI Drive does not accept `.eml`), so AI Drive receives the entire
message including attachments. No separate handling is needed.
Gmail's `raw` format returns the complete
[RFC 822](https://www.rfc-editor.org/rfc/rfc822) message bytes, which contain
the full MIME structure — body text, HTML alternative, and every attachment.

Because **AI Drive rejects `.eml` / `message/rfc822` uploads with HTTP 422**,
the script converts each email into a UTF-8 plain-text rendering before
uploading. The text version preserves the key headers (`From`, `To`, `Date`,
`Subject`, `Message-ID`) followed by the message body.

Attachments whose extension is in the AI Drive allow-list (`.pdf`, `.csv`,
`.xlsx`, `.xls`, `.docx`, `.doc`, `.pptx`, `.ppt`, `.txt`, `.md`) are queued
as **separate uploads** alongside the email text, so they remain accessible in
their native form. Attachments with unsupported extensions (images, archives,
etc.) are skipped — they remain visible inside Gmail.

## How it works

1. Refreshes Gmail credentials and ensures the `aidrive-archived` label exists.
2. Depending on `RUN_MODE`:
   - **historical** — generates up to 13 date-range chunks (12 complete past
     months plus the current partial month) and processes each in sequence.
   - **incremental** — computes a 2-day lookback window ending tomorrow.
   - **custom** — uses the supplied `START_DATE` / `END_DATE` directly.
3. For each window, lists message ids matching
   `after:START before:END -label:aidrive-archived`, capped at `MAX_EMAILS`.
4. For each message, fetches the raw RFC 822 bytes (includes attachments) and
   builds a filename: `YYYY-MM-DD_HHMM_<from>_<subject>_<msgid8>.txt`.
5. In batches of 25, requests signed upload URLs from
   `POST /signed_url_upload_batch_v2`.
6. PUTs each `.txt` blob (raw RFC 822 bytes) to the returned signed GCS URL.
4. For each message, fetches the raw RFC 822 bytes (includes attachments),
   renders it as plain text, and builds a filename:
   `YYYY-MM-DD_HHMM_<from>_<subject>_<msgid8>.txt`. Supported attachments are
   queued as additional `<...>-att<N>.<ext>` uploads in the same batch.
5. In batches of 25, requests signed upload URLs from
   `POST /signed_url_upload_batch_v2`.
6. PUTs each rendered text (or attachment) blob to the returned signed GCS URL.
7. **Labels the message in Gmail immediately on successful PUT** — this
   guarantees a rerun cannot upload the same bytes twice, even if the
   subsequent registration call fails.
8. Calls `POST /file_upload_status_v2` to register the upload with AI Drive.

### Failure handling and reruns

* **Retry classification** — Only transient failures are retried (HTTP 408 /
  429 / 5xx, connection resets, read/connect timeouts). Permanent failures
  fail fast and are never retried:
  - HTTP 401 / `AUTH_REQUIRED` / "invalid token" → treated as expired JWT,
    see [AI Drive JWT expiration](#ai-drive-jwt-expiration--refresh) below.
  - Other 4xx (validation errors, invalid payload, unsupported file type) →
    the offending record is skipped and counted as a failure; retrying would
    just produce the same error.
* All retried calls back off 5 seconds between attempts (up to 3 attempts).
* If `signed_url_upload_batch_v2` returns a different number of entries than
  requested, the entire batch is failed (no silent drops).
* If a GCS upload succeeds but registration fails, the message is still
  labeled in Gmail and counted as a failure. **Rerunning will not re-upload
  the bytes**; instead you should investigate the registration failure
  in the run log and re-register manually if needed.
* The job exits with a non-zero status if any failures occurred so the
  workflow run is marked red. **Exit code 2 is reserved** for clean
  termination after an expired JWT — see below.

## AI Drive JWT expiration & Firebase auto-refresh

AI Drive sits behind Firebase Authentication. The token used in
`Authorization: Bearer <JWT>` is a short-lived ID token (≈1 hour TTL) that
is normally minted in the browser by Firebase's secure-token endpoint. To
avoid stopping a long-running historical backfill every hour for a
manually-pasted token, the uploader can run that same refresh flow itself.

### How it works

1. Read `AIDRIVE_REFRESH_TOKEN` and `AIDRIVE_FIREBASE_API_KEY` from the
   environment (typically GitHub Actions secrets).
2. Before every AI Drive request, check the cached JWT's expiry. If it is
   missing or within ~2 minutes of expiring, POST to
   `https://securetoken.googleapis.com/v1/token?key=<API_KEY>` with
   `grant_type=refresh_token` and `refresh_token=<REFRESH_TOKEN>`.
3. Use the returned `id_token` as the new `Authorization: Bearer …`. The
   `expires_in` field (and the JWT's `exp` claim) drives the next refresh.
4. If AI Drive ever still responds with `HTTP 401` / `AUTH_REQUIRED` /
   "invalid token", trigger an immediate refresh and **retry the failing
   request once** before giving up.

The refresh token is long-lived and is never written to disk by the
uploader; the minted JWT is held in memory only and is never logged in
full (logs include only `event=firebase_refresh status=ok user_id=… ttl_secs=…`
plus a length-only redaction of the new token).

### Where to find the values (one-time, in the browser)

1. Open <https://myaidrive.com> in a browser and sign in.
2. Open **DevTools → Application → Local Storage → `https://myaidrive.com`**.
3. Find the entry whose key starts with `firebase:authUser:` and ends with
   `:[DEFAULT]` — the middle segment is the Firebase Web API key.
4. Read the JSON value:
   - `apiKey` → set as `AIDRIVE_FIREBASE_API_KEY`
   - `stsTokenManager.refreshToken` → set as `AIDRIVE_REFRESH_TOKEN`

That's it. After both values are saved as GitHub Actions secrets (or
written to `.env` for local runs), the uploader keeps itself authenticated
indefinitely without any further intervention.

### Fallback: manually-pasted token

If the Firebase secrets are not configured the uploader falls back to the
old behaviour: it reads `AIDRIVE_TOKEN` (or `AIDRIVE_TOKEN_FILE`) and
treats it as the bearer JWT until it expires. When AI Drive responds with
`HTTP 401` / `AUTH_REQUIRED` and no Firebase refresh is possible, the
script logs `AI Drive JWT expired — refresh required`, preserves the
historical checkpoint, and exits with **code 2** so you can paste a new
token and re-run.

To grab a fallback token: in the same DevTools view, copy
`stsTokenManager.accessToken` (or read it from `Authorization: Bearer …`
on any request to `ai-drive-api-prod-…run.app` under DevTools → Network).

### Replacing credentials

* **Refresh token rotated / revoked** — re-extract `AIDRIVE_REFRESH_TOKEN`
  from `firebase:authUser:…:[DEFAULT]` → `stsTokenManager.refreshToken`
  and update the GitHub Actions secret (or `.env`).
* **Manual fallback token expired** — paste a new value into `AIDRIVE_TOKEN`
  (Actions secret or `.env`), or write it to the file pointed to by
  `AIDRIVE_TOKEN_FILE` for hot-swap mid-run. Strongly consider switching
  to the Firebase auto-refresh flow above to avoid this entirely.

> ⚠️ **Never commit `.env`, `AIDRIVE_REFRESH_TOKEN`, `AIDRIVE_TOKEN`,
> any token file, or the checkpoint file.** `.env`, `*.checkpoint.json`,
> and `.backfill_checkpoint.json` are already listed in `.gitignore`. Any
> path you choose for `AIDRIVE_TOKEN_FILE` must also live outside the
> repository (e.g. under `/run/secrets/`).

## Resumable historical imports

Historical backfills can take a long time and can be interrupted by an
expired JWT, a network outage, GitHub Actions cancellation, or a manual
stop. The script writes a small JSON checkpoint file after every monthly
chunk that completes successfully, and **skips already-completed chunks
on the next run**:

* Default path: `.backfill_checkpoint.json` in the working directory.
* Override with the `CHECKPOINT_FILE` environment variable.
* The file contains only the list of completed `(start, end)` chunk keys
  plus a saved-at timestamp — no message data, no credentials.
* Writes are atomic (temp file + rename + `fsync`) so a crash mid-write
  cannot corrupt the file. A corrupt or unreadable checkpoint is ignored
  and treated as an empty resume state.
* The checkpoint is gitignored — never commit it.

To start a historical backfill from scratch, simply delete the checkpoint
file before triggering the run.

### Required configuration assumptions

* The Gmail OAuth token must hold the `gmail.modify` scope (needed both to
  read messages in `raw` format and to apply the processed label).
* The **Gmail API must be enabled** in the same Google Cloud project that
  owns the OAuth client (`GMAIL_CLIENT_ID`). If it isn't, the first Gmail
  call fails with HTTP 403 `accessNotConfigured`. Enable it at
  <https://console.developers.google.com/apis/api/gmail.googleapis.com/overview>
  and wait 1–2 minutes for propagation; the script retries the initial
  call to absorb that delay.
* AI Drive endpoints are hardcoded to
  `https://ai-drive-api-prod-qvg2narjsa-uc.a.run.app` —
  `signed_url_upload_batch_v2` and `file_upload_status_v2`. Update
  `AIDRIVE_BASE` in `email_backfill.py` if your account uses a different
  region/host.
* The Authorization scheme used by AI Drive is `Bearer <AIDRIVE_TOKEN>`.

## Local execution

For local runs, the recommended workflow is to use a `.env` file:

```bash
cp .env.example .env
# Edit .env: paste AIDRIVE_REFRESH_TOKEN + AIDRIVE_FIREBASE_API_KEY
# (preferred — auto-refresh) or, as a fallback, AIDRIVE_TOKEN.
# Plus your Gmail OAuth values from `get_gmail_token.py`.
pip install -r requirements.txt

# Export the variables into your shell, then run:
set -a; . ./.env; set +a
python email_backfill.py
```

> **⚠️ Warning:** AI Drive bearer JWTs are short-lived (~1 hour). When
> `AIDRIVE_REFRESH_TOKEN` and `AIDRIVE_FIREBASE_API_KEY` are configured,
> the uploader mints fresh JWTs automatically via Firebase's secure-token
> endpoint and a long historical run keeps going indefinitely. When only
> the manual `AIDRIVE_TOKEN` fallback is configured and AI Drive starts
> returning HTTP 401 / `AUTH_REQUIRED` / "invalid token", the script logs
> `AI Drive JWT expired — refresh required`, preserves any historical
> progress in `.backfill_checkpoint.json`, and exits with code 2. See
> [AI Drive JWT expiration & Firebase auto-refresh](#ai-drive-jwt-expiration--firebase-auto-refresh)
> for the full refresh procedure.
> **Never commit `.env`, raw tokens, or the checkpoint file** — they are
> already listed in `.gitignore`.

Or, if you prefer plain shell exports:

```bash
pip install -r requirements.txt
# Preferred: Firebase auto-refresh (no manual token rotation needed)
export AIDRIVE_REFRESH_TOKEN=...
export AIDRIVE_FIREBASE_API_KEY=AIzaSy...
# Fallback only — set instead of (or in addition to) the two above
# export AIDRIVE_TOKEN=...
export GMAIL_CLIENT_ID=...
export GMAIL_CLIENT_SECRET=...
export GMAIL_REFRESH_TOKEN=...

# Historical 12-month backfill
export RUN_MODE=historical
python email_backfill.py

# Incremental sync (last 2 days)
export RUN_MODE=incremental
python email_backfill.py

# Custom date range
export RUN_MODE=custom   # or leave unset
export START_DATE=2025/05/14
export END_DATE=2025/06/14
export MAX_EMAILS=2000          # optional
export AIDRIVE_FOLDER='04 - EMAIL ARCHIVE'   # optional
python email_backfill.py
```

## Limitations

* **Gmail query granularity** — Gmail's `after:` and `before:` filters are
  date-level only (no time-of-day precision). The incremental mode queries the
  last 2 full calendar days on every run; the `aidrive-archived` label ensures
  previously uploaded messages are never re-uploaded.
* **Historical backfill runtime** — A full 12-month backfill with thousands of
  emails can take tens of minutes. GitHub Actions has a 6-hour job limit, which
  is sufficient for typical mailbox sizes; very large mailboxes (100 k+ emails)
  may need to be split into smaller custom date-range runs.
* **Scheduled runs require secrets** — at minimum the script needs Gmail
  OAuth (`GMAIL_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`,
  `GMAIL_REFRESH_TOKEN`) plus AI Drive auth — either the preferred
  Firebase pair (`AIDRIVE_REFRESH_TOKEN` + `AIDRIVE_FIREBASE_API_KEY`) or
  the fallback `AIDRIVE_TOKEN`. If Gmail authentication stops working,
  renew the refresh token via `get_gmail_token.py`. If AI Drive auth
  stops working, re-extract the Firebase refresh token from the browser
  (see [Firebase auto-refresh](#ai-drive-jwt-expiration--firebase-auto-refresh)).
