# email_backfill.py

Automatically syncs Gmail messages into [My AI Drive](https://myaidrive.com) as
individual `.eml` files using the AI Drive signed-URL upload API.

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

Add the following under **Settings → Secrets and variables → Actions**:

| Secret | Description |
| --- | --- |
| `AIDRIVE_TOKEN` | Bearer JWT for your AI Drive account (copy from your AI Drive browser session). |
| `GMAIL_CLIENT_ID` | OAuth client id from step 2. |
| `GOOGLE_OAUTH_CLIENT_SECRET` | OAuth client secret from step 2. |
| `GMAIL_REFRESH_TOKEN` | Refresh token from step 2. |

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
`.eml` file, so AI Drive receives the entire message including attachments.
No separate handling is needed.

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
   builds a filename: `YYYY-MM-DD_HHMM_<from>_<subject>_<msgid8>.eml`.
5. In batches of 25, requests signed upload URLs from
   `POST /signed_url_upload_batch_v2`.
6. PUTs each `.eml` to the returned signed GCS URL.
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

## AI Drive JWT expiration & refresh

`AIDRIVE_TOKEN` is a Bearer JWT copied from a logged-in AI Drive browser
session. **These tokens expire periodically** (often after several hours),
which is shorter than a full historical backfill can run. The script
detects expiration and shuts down cleanly so you can refresh the token and
resume without losing progress.

### What expiration looks like

When AI Drive responds with HTTP 401, an `AUTH_REQUIRED` error code, or any
"invalid token" / "token expired" body, the script:

1. Logs `AI Drive JWT expired — refresh required` (plus a structured
   `event=auth_failure` line).
2. Stops the current batch cleanly — no further upload attempts are made
   with the dead token.
3. Preserves the resumable checkpoint (see below) so completed monthly
   chunks are not redone.
4. Exits with **exit code 2** (distinct from ordinary failures, which use
   exit code 1).

### Refreshing the token from browser DevTools

1. Open <https://myaidrive.com> in a browser and sign in.
2. Open **DevTools → Network**.
3. Trigger any action that calls the AI Drive API (e.g. open a folder).
4. Click any request to `ai-drive-api-prod-…run.app`.
5. In **Headers → Request Headers**, find `Authorization: Bearer <JWT>`.
6. Copy the JWT value (everything after `Bearer `, no quotes).

### Replacing the token

* **Local run with `.env`** — open `.env`, replace the value of
  `AIDRIVE_TOKEN`, save, and re-run. Never commit `.env`.
* **GitHub Actions** — go to **Settings → Secrets and variables → Actions**
  and update the `AIDRIVE_TOKEN` secret. The next workflow run will pick
  it up automatically.
* **Hot-swap mid-run (optional)** — set `AIDRIVE_TOKEN_FILE=/path/to/token`
  in the environment instead of (or in addition to) `AIDRIVE_TOKEN`. The
  script re-reads that file before every API request, so an operator (or
  an external token rotator) can drop in a fresh JWT while the process is
  running and the very next request will use it. The file must contain
  only the raw token — no `Bearer ` prefix, no surrounding quotes.

> ⚠️ **Never commit `.env`, `AIDRIVE_TOKEN`, or any token file.** Both
> `.env` and `*.checkpoint.json` are listed in `.gitignore`, but the
> external token-file path you choose for `AIDRIVE_TOKEN_FILE` must also
> live outside the repository (e.g. under `/run/secrets/` or a path you
> add to `.gitignore`).

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
# Edit .env and paste your AI Drive Bearer JWT into AIDRIVE_TOKEN,
# plus your Gmail OAuth values from `get_gmail_token.py`.
pip install -r requirements.txt

# Export the variables into your shell, then run:
set -a; . ./.env; set +a
python email_backfill.py
```

> **⚠️ Warning:** `AIDRIVE_TOKEN` is a Bearer JWT copied from your AI Drive
> browser session and **expires periodically**. If AI Drive API calls start
> failing with HTTP 401 / `AUTH_REQUIRED` / "invalid token", the script
> logs `AI Drive JWT expired — refresh required`, preserves any
> historical-mode progress in `.backfill_checkpoint.json`, and exits with
> code 2. See [AI Drive JWT expiration & refresh](#ai-drive-jwt-expiration--refresh)
> for the refresh procedure (browser DevTools → update `.env` or the
> `AIDRIVE_TOKEN` GitHub Actions secret → re-run).
> **Never commit `.env`, raw tokens, or the checkpoint file** — they are
> already listed in `.gitignore`.

Or, if you prefer plain shell exports:

```bash
pip install -r requirements.txt
export AIDRIVE_TOKEN=...
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
* **Scheduled runs require secrets** — If any of the four secrets
  (`AIDRIVE_TOKEN`, `GMAIL_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`,
  `GMAIL_REFRESH_TOKEN`) are missing or expired, scheduled runs will fail and
  show as red in the Actions tab. Renew the refresh token via `get_gmail_token.py`
  if Gmail authentication stops working.
