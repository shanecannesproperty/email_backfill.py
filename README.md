# email_backfill.py

Backfill Gmail messages into [My AI Drive](https://myaidrive.com) as
individual `.eml` files using the AI Drive signed-URL upload API.

The script is designed to run unattended in **GitHub Actions**, but also runs
locally. It is **idempotent**: each successfully uploaded message is tagged in
Gmail with the label `aidrive-archived`, and subsequent runs exclude that
label from the search query, so you can rerun the same date range safely.

## Repository contents

| File | Purpose |
| --- | --- |
| `email_backfill.py` | Main backfill job (Gmail → AI Drive). |
| `get_gmail_token.py` | One-time local helper to mint a Gmail OAuth refresh token. |
| `.github/workflows/email_backfill.yml` | Scheduled + manual workflow that runs the backfill in safe date chunks. |
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
success it prints `GMAIL_CLIENT_ID`, `GMAIL_CLIENT_SECRET`, and
`GMAIL_REFRESH_TOKEN`. Delete `credentials.json` afterwards.

### 3. Configure GitHub repository secrets

Add the following under **Settings → Secrets and variables → Actions**:

| Secret | Description |
| --- | --- |
| `AIDRIVE_API_KEY` | API key for your AI Drive account. |
| `GMAIL_CLIENT_ID` | OAuth client id from step 2. |
| `GMAIL_CLIENT_SECRET` | OAuth client secret from step 2. |
| `GMAIL_REFRESH_TOKEN` | Refresh token from step 2. |

## Running the backfill

Open the **Actions** tab → **Email Backfill** → **Run workflow**.

If you click **Run workflow** with defaults, the job immediately processes the
**rolling last 12 months** in month-sized ranges, one range at a time.

You can also provide inputs:

| Input | Required | Default | Notes |
| --- | --- | --- | --- |
| `start_date` | no | — | Optional manual override. Must be supplied with `end_date`. Format `YYYY/MM/DD` (Gmail-search syntax). |
| `end_date` | no | — | Optional manual override. Must be supplied with `start_date`. Format `YYYY/MM/DD` (exclusive upper bound). |
| `months_back` | no | `12` | Used only when `start_date`/`end_date` are omitted. |
| `max_emails` | no | `5000` | Hard cap on messages processed per run. |
| `aidrive_folder` | no | `04 - EMAIL ARCHIVE` | Top-level AI Drive folder. Files land in `<folder>/YYYY-MM/`. |

The workflow also runs on a daily schedule and uses the same rolling-window
logic. Each run prints every generated range in logs, then logs each range as a
separate grouped section so you can see exactly what was processed and whether
the workflow continued after any failed range.

Reruns of the same range are safe because already processed messages are skipped
via the `aidrive-archived` label.

## How it works

1. Refreshes Gmail credentials and ensures the `aidrive-archived` label exists.
2. Lists message ids matching
   `after:START_DATE before:END_DATE -label:aidrive-archived`, capped at
   `MAX_EMAILS`.
3. For each message, fetches the raw RFC 822 bytes and builds a filename of
   the form `YYYY-MM-DD_HHMM_<from>_<subject>_<msgid8>.eml`.
4. In batches of 25, requests signed upload URLs from
   `POST /signed_url_upload_batch_v2`.
5. PUTs each `.eml` to the returned signed GCS URL.
6. **Labels the message in Gmail immediately on successful PUT** — this is
   intentional: it guarantees a rerun cannot upload the same bytes twice,
   even if the subsequent registration call fails.
7. Calls `POST /file_upload_status_v2` to register the upload with AI Drive.

### Failure handling and reruns

* All AI Drive API calls and GCS PUTs are retried up to 3 times with a
  5-second backoff. Network errors are treated as retryable.
* If `signed_url_upload_batch_v2` returns a different number of entries than
  requested, the entire batch is failed (no silent drops).
* If a GCS upload succeeds but registration fails, the message is still
  labeled in Gmail and counted as a failure. **Rerunning will not re-upload
  the bytes**; instead you should investigate the registration failure
  in the run log and re-register manually if needed.
* The job exits with a non-zero status if any failures occurred so the
  workflow run is marked red.

### Required configuration assumptions

* The Gmail OAuth token must hold the `gmail.modify` scope (needed both to
  read messages in `raw` format and to apply the processed label).
* AI Drive endpoints are hardcoded to
  `https://ai-drive-api-prod-qvg2narjsa-uc.a.run.app` —
  `signed_url_upload_batch_v2` and `file_upload_status_v2`. Update
  `AIDRIVE_BASE` in `email_backfill.py` if your account uses a different
  region/host.
* The Authorization scheme used by AI Drive is `Bearer <AIDRIVE_API_KEY>`.

## Local execution

```bash
pip install -r requirements.txt
export AIDRIVE_API_KEY=...
export GMAIL_CLIENT_ID=...
export GMAIL_CLIENT_SECRET=...
export GMAIL_REFRESH_TOKEN=...
export START_DATE=2025/05/14
export END_DATE=2025/06/14
export MAX_EMAILS=5000          # optional
export AIDRIVE_FOLDER='04 - EMAIL ARCHIVE'   # optional
python email_backfill.py
```
