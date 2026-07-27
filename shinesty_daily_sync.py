#!/usr/bin/env python3
"""
Shinesty Daily Sync — MFT_New Customer Creation Board
======================================================
Standalone version — runs on any schedule (cron, GitHub Actions, systemd timer),
independent of the Claude Cowork scheduler.

FILTER: Process rows where Import NetSuite Status = Done (label 1)
        AND NS Customer Link is populated.
        "Done" = the NS customer record has been created and is ready for doc upload.

What it does, per qualifying row:
  1. Resale Certificate  → extract resale # + expiration → write to NS ONLY
                         → upload file to NS File Cabinet
  2. Signature file      → upload to NS File Cabinet
  3. Multiple Address    → extract addresses → add to NS customer record
                         → Slack alert to #whsl_multiple_address_alert @Alex
  4. Status update:
       All steps OK  → Import NetSuite Status = "Docs Uploaded"
       Any error     → Import NetSuite Status = "Error" + post update on item

Required environment variables (set these as secrets in whatever scheduler
runs this — GitHub Actions secrets, systemd EnvironmentFile, AWS Secrets
Manager, etc. NEVER commit real values to a repo or plaintext file):
  MONDAY_API_KEY        — Monday.com API token
  NS_ACCOUNT_ID         — NetSuite account ID (e.g. 4775967)
  NS_CONSUMER_KEY       — NetSuite OAuth consumer key
  NS_CONSUMER_SECRET    — NetSuite OAuth consumer secret
  NS_TOKEN_ID           — NetSuite OAuth token ID
  NS_TOKEN_SECRET       — NetSuite OAuth token secret
  ANTHROPIC_API_KEY     — Anthropic API key (for doc extraction)
  SLACK_BOT_TOKEN       — Slack bot token
  NS_RESTLET_URL        — (optional) NetSuite RESTlet URL for file uploads

Status label IDs (confirmed 2026-04-07): Done=1, Error=2, Docs Uploaded=4, Missing Resale Cert=8
"""

import os
import re
import json
import base64
import logging
import hashlib
import hmac
import time
import urllib.parse
from datetime import datetime
from typing import Optional

import requests

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("shinesty_sync")

# ──────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────
MONDAY_BOARD_ID     = 18402152636
SLACK_CHANNEL_ID    = "C0ARSBBPGP7"   # #whsl_multiple_address_alert
SLACK_ALEX_USER_ID  = "U02H69DED2N"   # Alex — Wholesale Sales Ops Coordinator

# Monday status label IDs
STATUS_DONE                = 1   # "Done" — NS record created; triggers this script
STATUS_ERROR               = 2   # "Error"
STATUS_DOCS_UPLOADED       = 4   # "Docs Uploaded" — label ID 4, confirmed from board settings
STATUS_MISSING_RESALE_CERT = 8   # "Missing Resale Cert" — no cert uploaded, or uploaded doc isn't a valid resale cert

# Monday column IDs (from board inspection)
COL_STATUS       = "status"
COL_NS_LINK      = "link_mm1j2zkk"
COL_RESALE_FILE  = "upload_file__1"
COL_SIGNATURE    = "signature__1"
COL_MULTI_ADDR   = "upload_file6__1"
COL_COMPANY_NAME = "short_text1__1"

# ──────────────────────────────────────────────
# ENV VARS
# ──────────────────────────────────────────────
MONDAY_API_KEY     = os.environ["MONDAY_API_KEY"]
NS_RESTLET_URL     = os.environ.get("NS_RESTLET_URL", "")  # optional until SuiteScript is deployed
NS_ACCOUNT_ID      = os.environ["NS_ACCOUNT_ID"]
NS_CONSUMER_KEY    = os.environ["NS_CONSUMER_KEY"]
NS_CONSUMER_SECRET = os.environ["NS_CONSUMER_SECRET"]
NS_TOKEN_ID        = os.environ["NS_TOKEN_ID"]
NS_TOKEN_SECRET    = os.environ["NS_TOKEN_SECRET"]
ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]
SLACK_BOT_TOKEN    = os.environ["SLACK_BOT_TOKEN"]


# ══════════════════════════════════════════════
# MONDAY.COM HELPERS
# ══════════════════════════════════════════════

def monday_gql(query: str, variables: dict = None) -> dict:
    resp = requests.post(
        "https://api.monday.com/v2",
        headers={
            "Authorization": MONDAY_API_KEY,
            "Content-Type": "application/json",
            "API-Version": "2024-01",
        },
        json={"query": query, "variables": variables or {}},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"Monday GraphQL error: {data['errors']}")
    return data["data"]


def get_pending_items() -> list:
    query = """
    query ($boardId: ID!, $cursor: String) {
      boards(ids: [$boardId]) {
        items_page(
          limit: 50,
          cursor: $cursor,
          query_params: {
            rules: [
              { column_id: "status", compare_value: [1], operator: any_of }
              { column_id: "link_mm1j2zkk", compare_value: [], operator: is_not_empty }
            ]
          }
        ) {
          cursor
          items {
            id
            name
            column_values(ids: [
              "status",
              "link_mm1j2zkk",
              "upload_file__1",
              "signature__1",
              "upload_file6__1",
              "short_text1__1"
            ]) {
              id
              text
              value
            }
          }
        }
      }
    }
    """
    items = []
    cursor = None
    while True:
        data = monday_gql(query, {"boardId": str(MONDAY_BOARD_ID), "cursor": cursor})
        page = data["boards"][0]["items_page"]
        items.extend(page["items"])
        cursor = page.get("cursor")
        if not cursor:
            break
    log.info(f"Found {len(items)} item(s) ready for doc upload.")
    return items


def get_col(item: dict, col_id: str) -> dict:
    for cv in item["column_values"]:
        if cv["id"] == col_id:
            return cv
    return {}


def get_file_urls(item: dict, col_id: str) -> list:
    cv = get_col(item, col_id)
    if not cv.get("value"):
        return []
    try:
        files = json.loads(cv["value"]).get("files", [])
        result = []
        for f in files:
            asset_id = f.get("assetId") or f.get("asset_id")
            if asset_id:
                q = "query ($ids: [ID!]!) { assets(ids: $ids) { id name public_url file_extension } }"
                assets = monday_gql(q, {"ids": [str(asset_id)]}).get("assets", [])
                if assets:
                    a = assets[0]
                    result.append({
                        "name": a["name"],
                        "url":  a["public_url"],
                        "ext":  (a.get("file_extension") or "").lower().lstrip("."),
                    })
        return result
    except Exception as e:
        log.warning(f"Could not parse file column {col_id}: {e}")
        return []


def set_monday_status(item_id: str, label_id: int):
    mutation = """
    mutation ($boardId: ID!, $itemId: ID!, $value: JSON!) {
      change_column_value(board_id: $boardId, item_id: $itemId,
                          column_id: "status", value: $value) { id }
    }
    """
    monday_gql(mutation, {
        "boardId": str(MONDAY_BOARD_ID),
        "itemId": str(item_id),
        "value": json.dumps({"index": label_id}),
    })


def post_monday_update(item_id: str, message: str):
    mutation = """
    mutation ($itemId: ID!, $body: String!) {
      create_update(item_id: $itemId, body: $body) { id }
    }
    """
    monday_gql(mutation, {"itemId": str(item_id), "body": message})


# ══════════════════════════════════════════════
# NETSUITE HELPERS  (OAuth 1.0 HMAC-SHA256)
# ══════════════════════════════════════════════

def _ns_oauth_header(method: str, url: str) -> str:
    timestamp = str(int(time.time()))
    nonce     = hashlib.md5(f"{timestamp}{NS_TOKEN_ID}".encode()).hexdigest()

    oauth_params = {
        "oauth_consumer_key":     NS_CONSUMER_KEY,
        "oauth_nonce":            nonce,
        "oauth_signature_method": "HMAC-SHA256",
        "oauth_timestamp":        timestamp,
        "oauth_token":            NS_TOKEN_ID,
        "oauth_version":          "1.0",
    }

    param_str = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in sorted(oauth_params.items())
    )

    base = "&".join([
        method.upper(),
        urllib.parse.quote(url, safe=""),
        urllib.parse.quote(param_str, safe=""),
    ])

    signing_key = f"{urllib.parse.quote(NS_CONSUMER_SECRET, safe='')}&{urllib.parse.quote(NS_TOKEN_SECRET, safe='')}"

    signature = base64.b64encode(
        hmac.new(signing_key.encode(), base.encode(), hashlib.sha256).digest()
    ).decode()

    oauth_params["oauth_signature"] = signature
    parts = ", ".join(
        f'{k}="{urllib.parse.quote(v, safe="")}"'
        for k, v in sorted(oauth_params.items()) if k.startswith("oauth_")
    )
    return
