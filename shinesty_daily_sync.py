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

Status label IDs (confirmed 2026-04-07): Done=1, Error=2, Docs Uploaded=4
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
TAG_ALEX_IN_SLACK   = False           # set True to actually @-mention Alex; False while testing

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
    return f'OAuth realm="{NS_ACCOUNT_ID}", {parts}'


def _ns_oauth_header_restlet(method: str, base_url: str, query_string: str) -> str:
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

    all_params = dict(oauth_params)
    if query_string:
        for part in query_string.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                all_params[urllib.parse.unquote(k)] = urllib.parse.unquote(v)

    param_str = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(str(v), safe='')}"
        for k, v in sorted(all_params.items())
    )

    base = "&".join([
        method.upper(),
        urllib.parse.quote(base_url, safe=""),
        urllib.parse.quote(param_str, safe=""),
    ])

    signing_key = f"{urllib.parse.quote(NS_CONSUMER_SECRET, safe='')}&{urllib.parse.quote(NS_TOKEN_SECRET, safe='')}"
    signature = base64.b64encode(
        hmac.new(signing_key.encode(), base.encode(), hashlib.sha256).digest()
    ).decode()

    oauth_params["oauth_signature"] = signature
    parts = ", ".join(
        f'{k}="{urllib.parse.quote(str(v), safe="")}"'
        for k, v in sorted(oauth_params.items()) if k.startswith("oauth_")
    )
    return f'OAuth realm="{NS_ACCOUNT_ID}", {parts}'


def _ns_base() -> str:
    return f"https://{NS_ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest"


def ns_get(path: str, params: dict = None) -> dict:
    base_url = f"{_ns_base()}/record/v1{path}"
    r = requests.get(base_url, params=params or {}, headers={
        "Authorization": _ns_oauth_header("GET", base_url),
        "Content-Type": "application/json",
    }, timeout=30)
    if not r.ok:
        log.error(f"NS GET error {r.status_code}: {r.text}")
    r.raise_for_status()
    return r.json()


def ns_patch(path: str, body: dict) -> None:
    url = f"{_ns_base()}/record/v1{path}"
    r = requests.patch(url, headers={
        "Authorization": _ns_oauth_header("PATCH", url),
        "Content-Type": "application/json",
    }, json=body, timeout=30)
    r.raise_for_status()


def ns_post_record(path: str, body: dict) -> requests.Response:
    url = f"{_ns_base()}/record/v1{path}"
    r = requests.post(url, headers={
        "Authorization": _ns_oauth_header("POST", url),
        "Content-Type": "application/json",
    }, json=body, timeout=60)
    r.raise_for_status()
    return r


def ns_get_customer_id_from_link(ns_link: str) -> Optional[str]:
    m = re.search(r"[?&]id=(\d+)", ns_link)
    if m:
        return m.group(1)
    m = re.search(r"/customer/(\d+)", ns_link)
    return m.group(1) if m else None


def ns_update_customer_fields(customer_id: str, fields: dict) -> None:
    ns_patch(f"/customer/{customer_id}", fields)


def ns_add_customer_addresses(customer_id: str, addresses: list) -> None:
    record = ns_get(f"/customer/{customer_id}", params={"expandSubResources": "true"})
    addressbook = record.get("addressbook", {}).get("items", [])

    for address in addresses:
        addressbook.append({
            "addressbookaddress": {
                "addr1":   address.get("addr1", ""),
                "addr2":   address.get("addr2", ""),
                "city":    address.get("city", ""),
                "state":   address.get("state", ""),
                "zip":     address.get("zip", ""),
                "country": {"id": address.get("country", "US")},
            },
            "label":           address.get("label", address.get("addr1", "Address")),
            "defaultShipping": address.get("defaultShipping", False),
            "defaultBilling":  address.get("defaultBilling", False),
        })

    ns_patch(f"/customer/{customer_id}", {"addressbook": {"items": addressbook}})


def ns_upload_file_via_restlet(customer_id: str, filename: str,
                               file_bytes: bytes, content_type: str,
                               file_type: str) -> str:
    if not NS_RESTLET_URL:
        log.info("      NS_RESTLET_URL not set — skipping file upload (deploy SuiteScript first)")
        return ""

    payload = {
        "customer_id":  customer_id,
        "file_name":    filename,
        "file_content": base64.b64encode(file_bytes).decode(),
        "media_type":   content_type,
        "file_type":    file_type,
    }
    base_url = NS_RESTLET_URL.split("?")[0]
    query_string = NS_RESTLET_URL.split("?")[1] if "?" in NS_RESTLET_URL else ""
    r = requests.post(NS_RESTLET_URL, headers={
        "Authorization": _ns_oauth_header_restlet("POST", base_url, query_string),
        "Content-Type":  "application/json",
    }, json=payload, timeout=60)
    if not r.ok:
        log.error(f"      RESTlet error {r.status_code}: {r.text[:300]}")
        r.raise_for_status()
    result = r.json()
    if result.get("success"):
        log.info(f"      File uploaded to NS — file_id: {result.get('file_id')} folder_id: {result.get('folder_id')}")
        return result.get("file_id", "")
    else:
        raise RuntimeError(f"RESTlet upload failed: {result.get('error')}")


# ══════════════════════════════════════════════
# ANTHROPIC HELPERS
# ══════════════════════════════════════════════

MEDIA_TYPES = {
    "pdf":  "application/pdf",
    "png":  "image/png",
    "jpg":  "image/jpeg",
    "jpeg": "image/jpeg",
    "gif":  "image/gif",
    "webp": "image/webp",
}


def _convert_heic_to_jpeg(file_bytes: bytes) -> tuple:
    """Convert HEIC/HEIF (iPhone photo format) to JPEG bytes, since Claude's
    API doesn't accept HEIC directly. Returns (jpeg_bytes, 'jpg')."""
    import io
    import pillow_heif
    from PIL import Image

    pillow_heif.register_heif_opener()
    img = Image.open(io.BytesIO(file_bytes)).convert("RGB")
    out = io.BytesIO()
    img.save(out, format="JPEG", quality=90)
    return out.getvalue(), "jpg"


def normalize_heic_file(file_bytes: bytes, ext: str, filename: str) -> tuple:
    """If the file is HEIC/HEIF (iPhone photo format), convert it to JPEG so
    both the copy sent to Claude AND the copy stored in NetSuite are a normal,
    viewable image format. Returns (file_bytes, ext, filename) — unchanged if
    not HEIC/HEIF, or if conversion fails (falls back to original file)."""
    if ext not in ("heic", "heif"):
        return file_bytes, ext, filename
    try:
        converted_bytes, new_ext = _convert_heic_to_jpeg(file_bytes)
        new_filename = re.sub(r"\.(heic|heif)$", "", filename, flags=re.IGNORECASE) + f".{new_ext}"
        log.info(f"      Converted {filename} → {new_filename} for NetSuite/Claude compatibility")
        return converted_bytes, new_ext, new_filename
    except Exception as e:
        log.warning(f"      HEIC conversion failed ({e}) — keeping original file as-is")
        return file_bytes, ext, filename


def _file_block(file_bytes: bytes, ext: str) -> dict:
    if ext in ("heic", "heif"):
        try:
            file_bytes, ext = _convert_heic_to_jpeg(file_bytes)
        except Exception as e:
            log.warning(f"      HEIC conversion failed ({e}) — sending as-is, Claude may reject it")
    mt = MEDIA_TYPES.get(ext, "application/octet-stream")
    data = base64.b64encode(file_bytes).decode()
    if ext == "pdf":
        return {"type": "document", "source": {"type": "base64", "media_type": mt, "data": data}}
    return {"type": "image", "source": {"type": "base64", "media_type": mt, "data": data}}


def _claude(messages: list, max_tokens: int = 512) -> str:
    for attempt in range(5):
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={"model": "claude-sonnet-5", "max_tokens": max_tokens, "messages": messages},
            timeout=90,
        )
        if resp.status_code == 429:
            wait = 30 * (attempt + 1)
            log.warning(f"      Claude rate limit hit — waiting {wait}s before retry (attempt {attempt+1}/4)")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        content_blocks = resp.json()["content"]
        text = next(b["text"] for b in content_blocks if b.get("type") == "text").strip()
        return re.sub(r"```json|```", "", text).strip()
    raise RuntimeError("Claude API rate limit: max retries exceeded after 4 attempts (~5 min total wait)")


def claude_extract_resale_cert(file_bytes: bytes, ext: str) -> dict:
    prompt = (
        "You are reviewing a document uploaded to a wholesale resale certificate field. "
        "Your job is to:\n"
        "1. Identify what type of document this is\n"
        "2. Determine if it is acceptable as proof of resale authorization\n"
        "3. Extract the certificate/license number and expiration date if present\n\n"
        "ACCEPTABLE documents (is_valid=true):\n"
        "- Resale certificate (any US state format, including multi-state)\n"
        "- Sales tax exemption certificate\n"
        "- State sales tax license or permit\n"
        "- Retailer's sales tax registration\n"
        "- Business license that includes resale or tax authorization\n\n"
        "NOT ACCEPTABLE (is_valid=false):\n"
        "- W-9 (IRS form requesting taxpayer ID — has 'Request for Taxpayer' header, "
        "TIN fields, certification signature block)\n"
        "- Any document that is clearly not related to resale or sales tax authorization\n\n"
        "Respond ONLY with valid JSON, no markdown, no extra text:\n"
        '{"document_type": "<resale_certificate|sales_tax_license|business_license|w9|unknown>", '
        '"is_valid": <true|false>, '
        '"reject_reason": "<reason if is_valid=false, else null>", '
        '"resale_number": "<certificate or license number, or null>", '
        '"expiration_date": "<YYYY-MM-DD or null>"}'
    )
    text = _claude([{"role": "user", "content": [
        _file_block(file_bytes, ext),
        {"type": "text", "text": prompt},
    ]}], max_tokens=512)
    return json.loads(text)


def claude_extract_addresses(file_bytes: bytes, ext: str) -> list:
    text_content = None

    if ext in ("xlsx", "xls"):
        try:
            import io
            if ext == "xlsx":
                import openpyxl
                wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True)
                rows = []
                for ws in wb.worksheets:
                    for row in ws.iter_rows(values_only=True):
                        rows.append("\t".join("" if c is None else str(c) for c in row))
                text_content = "\n".join(rows)
            else:
                try:
                    import xlrd
                    wb = xlrd.open_workbook(file_contents=file_bytes)
                    rows = []
                    for ws in wb.sheets():
                        for i in range(ws.nrows):
                            rows.append("\t".join(str(ws.cell_value(i, j)) for j in range(ws.ncols)))
                    text_content = "\n".join(rows)
                except ImportError:
                    log.warning("xlrd not installed — install with: pip install xlrd")
                    text_content = None
        except Exception as e:
            log.warning(f"Excel read error: {e} — sending as base64")
    elif ext == "docx":
        try:
            import io
            from docx import Document
            text_content = "\n".join(p.text for p in Document(io.BytesIO(file_bytes)).paragraphs)
        except ImportError:
            log.warning("python-docx not installed — sending as base64")
    elif ext == "csv":
        text_content = file_bytes.decode("utf-8", errors="replace")

    prompt = (
        "Extract ALL addresses from the following. "
        "Respond ONLY with a JSON array, no markdown:\n"
        '[{"addr1":"...","addr2":"...","city":"...","state":"...(2-letter)",'
        '"zip":"...","country":"US","label":"store name or street"}]'
    )

    if text_content:
        messages = [{"role": "user", "content": f"{prompt}\n\nDOCUMENT:\n{text_content}"}]
    else:
        messages = [{"role": "user", "content": [
            _file_block(file_bytes, ext),
            {"type": "text", "text": prompt},
        ]}]

    return json.loads(_claude(messages, max_tokens=1024))


# ══════════════════════════════════════════════
# SLACK HELPER
# ══════════════════════════════════════════════

def slack_post(message: str) -> None:
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json"},
        json={"channel": SLACK_CHANNEL_ID, "text": message, "mrkdwn": True},
        timeout=15,
    )
    r.raise_for_status()
    result = r.json()
    if not result.get("ok"):
        raise RuntimeError(f"Slack error: {result.get('error')}")


def slack_upload_file(filename: str, file_bytes: bytes, initial_comment: str) -> None:
    """Upload a file directly into the Slack channel (rather than posting a
    link to it), using Slack's 3-step external upload flow."""
    r1 = requests.post(
        "https://slack.com/api/files.getUploadURLExternal",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        data={"filename": filename, "length": len(file_bytes)},
        timeout=15,
    )
    r1.raise_for_status()
    d1 = r1.json()
    if not d1.get("ok"):
        raise RuntimeError(f"Slack getUploadURLExternal error: {d1.get('error')}")
    upload_url = d1["upload_url"]
    file_id = d1["file_id"]

    r2 = requests.post(upload_url, files={"file": (filename, file_bytes)}, timeout=60)
    r2.raise_for_status()

    r3 = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json"},
        json={
            "files": [{"id": file_id, "title": filename}],
            "channel_id": SLACK_CHANNEL_ID,
            "initial_comment": initial_comment,
        },
        timeout=30,
    )
    r3.raise_for_status()
    d3 = r3.json()
    if not d3.get("ok"):
        raise RuntimeError(f"Slack completeUploadExternal error: {d3.get('error')}")


# ══════════════════════════════════════════════
# MAIN ITEM PROCESSOR
# ══════════════════════════════════════════════

def process_item(item: dict) -> None:
    item_id      = item["id"]
    item_name    = item["name"]
    errors       = []
    missing_cert_reasons = []

    log.info(f"── [{item_id}] {item_name}")

    ns_link_col = get_col(item, COL_NS_LINK)
    try:
        ns_link_url = json.loads(ns_link_col.get("value") or "{}").get("url", "")
    except Exception:
        ns_link_url = ns_link_col.get("text", "")

    customer_id = ns_get_customer_id_from_link(ns_link_url)
    if not customer_id:
        raise ValueError(f"Cannot extract NS customer ID from: {ns_link_url!r}")

    company_name = get_col(item, COL_COMPANY_NAME).get("text", item_name).strip()

    resale_files = get_file_urls(item, COL_RESALE_FILE)
    if resale_files:
        for f in resale_files:
            try:
                log.info(f"  [1] Resale cert: {f['name']}")
                fb = download_file(f["url"])
                fb, f["ext"], f["name"] = normalize_heic_file(fb, f["ext"], f["name"])
                extracted = claude_extract_resale_cert(fb, f["ext"])
                log.info(f"      Document type: {extracted.get('document_type')} | valid: {extracted.get('is_valid')}")

                if not extracted.get("is_valid"):
                    reject_reason = extracted.get("reject_reason") or f"Unacceptable document type: {extracted.get('document_type', 'unknown')}"
                    msg = f"\"{f['name']}\" is not a valid resale certificate: {reject_reason}"
                    log.error(f"      REJECTED: {reject_reason}")
                    missing_cert_reasons.append(msg)
                    continue

                ns_fields = {}
                if extracted.get("resale_number"):
                    ns_fields["resalenumber"] = extracted["resale_number"]
                if extracted.get("expiration_date"):
                    ns_fields["custentity18"] = extracted["expiration_date"]
                if ns_fields:
                    ns_update_customer_fields(customer_id, ns_fields)
                    log.info(f"      NS {customer_id} ← {ns_fields}")

                    if ns_fields.get("resalenumber"):
                        tax_fields = {
                            "taxable": False,
                            "taxitem": {"id": "462879"},
                        }
                        ns_update_customer_fields(customer_id, tax_fields)
                        log.info(f"      NS {customer_id} ← tax exempt (taxable=False, taxitem=Shinesty Not Taxable [462879])")
                else:
                    log.info(f"      Valid cert but no number/expiry found to write")

                ct = MEDIA_TYPES.get(f["ext"], "application/octet-stream")
                ns_upload_file_via_restlet(customer_id, f["name"], fb, ct, "resale_cert")

            except Exception as e:
                msg = f"Step 1 resale cert '{f['name']}': {e}"
                log.error(f"      ERROR: {msg}")
                errors.append(msg)
    else:
        log.info("  [1] No resale certificate — skipping")
        missing_cert_reasons.append("No resale certificate has been uploaded to this item.")

    sig_files = get_file_urls(item, COL_SIGNATURE)
    if sig_files:
        for f in sig_files:
            try:
                log.info(f"  [2] Signature: {f['name']}")
                fb = download_file(f["url"])
                fb, f["ext"], f["name"] = normalize_heic_file(fb, f["ext"], f["name"])
                ct = MEDIA_TYPES.get(f["ext"], "application/octet-stream")
                ns_upload_file_via_restlet(customer_id, f["name"], fb, ct, "signature")
            except Exception as e:
                msg = f"Step 2 signature '{f['name']}': {e}"
                log.error(f"      ERROR: {msg}")
                errors.append(msg)
    else:
        log.info("  [2] No signature file — skipping")

    addr_files = get_file_urls(item, COL_MULTI_ADDR)
    if addr_files:
        for f in addr_files:
            try:
                log.info(f"  [3] Multi-address: {f['name']}")
                fb        = download_file(f["url"])
                fb, f["ext"], f["name"] = normalize_heic_file(fb, f["ext"], f["name"])
                addresses = claude_extract_addresses(fb, f["ext"])
                log.info(f"      Extracted {len(addresses)} address(es)")
                ns_add_customer_addresses(customer_id, addresses)
                for addr in addresses:
                    log.info(f"      Added: {addr.get('addr1')}, {addr.get('city')}, {addr.get('state')}")

                ct = MEDIA_TYPES.get(f["ext"], "application/octet-stream")
                ns_upload_file_via_restlet(customer_id, f["name"], fb, ct, "multiple_address")

                addr_lines = "\n".join(
                    f"  • {a.get('label','')}: {a.get('addr1','')} {a.get('addr2','').strip()}, "
                    f"{a.get('city','')}, {a.get('state','')} {a.get('zip','')}"
                    for a in addresses
                )
                mention = f"<@{SLACK_ALEX_USER_ID}> — " if TAG_ALEX_IN_SLACK else ""
                slack_upload_file(
                    f["name"],
                    fb,
                    initial_comment=(
                        f"{mention}Multiple addresses added to a customer record.\n\n"
                        f"*Customer:* {company_name}\n"
                        f"*NS Record:* {ns_link_url}\n"
                        f"*Addresses added ({len(addresses)}):*\n{addr_lines}\n\n"
                        f"Please verify against the attached file."
                    ),
                )
                log.info("      Slack alert sent (file attached)")
            except Exception as e:
                msg = f"Step 3 multi-address '{f['name']}': {e}"
                log.error(f"      ERROR: {msg}")
                errors.append(msg)
    else:
        log.info("  [3] No multiple address file — skipping")

    if errors:
        note = (
            f"⚠️ Sync errors ({datetime.now().strftime('%Y-%m-%d')}):\n"
            + "\n".join(f"• {e}" for e in errors)
        )
        if missing_cert_reasons:
            note += "\n\nAlso flagged:\n" + "\n".join(f"• {m}" for m in missing_cert_reasons)
        set_monday_status(item_id, STATUS_ERROR)
        post_monday_update(item_id, note)
        log.warning(f"  → ERROR ({len(errors)} issue(s))")
    elif missing_cert_reasons:
        set_monday_status(item_id, STATUS_MISSING_RESALE_CERT)
        post_monday_update(
            item_id,
            f"📋 Missing Resale Cert ({datetime.now().strftime('%Y-%m-%d')}):\n"
            + "\n".join(f"• {m}" for m in missing_cert_reasons)
            + "\n\nPlease upload a valid resale certificate to this item's Resale Certificate field, "
              "then change the status back to \"Done\" — this will automatically re-trigger the sync "
              "and update the customer record in NetSuite."
        )
        log.warning(f"  → MISSING RESALE CERT ({len(missing_cert_reasons)} issue(s))")
    else:
        set_monday_status(item_id, STATUS_DOCS_UPLOADED)
        log.info("  → DOCS UPLOADED ✓")


def download_file(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


# ══════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════

def main():
    log.info("=" * 60)
    log.info(f"Shinesty Daily Sync  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    items = get_pending_items()
    if not items:
        log.info("No items to process today.")
        return

    ok = fail = 0
    for item in items:
        try:
            process_item(item)
            ok += 1
        except Exception as e:
            fail += 1
            iid = item.get("id", "?")
            log.error(f"Fatal error on {iid}: {e}", exc_info=True)
            try:
                set_monday_status(iid, STATUS_ERROR)
                post_monday_update(iid, f"⚠️ Fatal error ({datetime.now().strftime('%Y-%m-%d')}):\n{e}")
            except Exception:
                pass

    log.info("=" * 60)
    log.info(f"Complete.  ✓ {ok} succeeded   ✗ {fail} errored")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
