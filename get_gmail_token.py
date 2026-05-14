"""
get_gmail_token.py

ONE-TIME local helper to obtain a Gmail refresh token.

Run this ONCE on your Mac. It will:
1. Open a browser window for you to log into Gmail and approve access
2. Print a refresh token

Paste the refresh token (plus your client id and client secret) into
GitHub Secrets as GMAIL_REFRESH_TOKEN, GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET.

Prerequisite: A credentials.json file in the same folder, downloaded
from Google Cloud Console (OAuth 2.0 Client ID, type "Desktop app").

USAGE:
  pip install google-auth-oauthlib google-api-python-client
  python get_gmail_token.py
"""

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]


def main():
    flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
    creds = flow.run_local_server(port=0, prompt="consent", access_type="offline")
    print("\n=== COPY THESE VALUES INTO GITHUB SECRETS ===\n")
    print(f"GMAIL_CLIENT_ID:\n{creds.client_id}\n")
    print(f"GMAIL_CLIENT_SECRET:\n{creds.client_secret}\n")
    print(f"GMAIL_REFRESH_TOKEN:\n{creds.refresh_token}\n")
    print("Done. You can delete credentials.json after this.")


if __name__ == "__main__":
    main()
