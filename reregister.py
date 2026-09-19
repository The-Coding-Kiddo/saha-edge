#!/usr/bin/env python3
# ==============================================================================
# SAHA LIVE - Re-register missing matches from R2
# ==============================================================================
# Pulls R2 credentials from the backend (same as the ingestor does),
# lists all folders in R2 for this venue, and registers any that are missing.
# Safe to run multiple times — 409s are skipped silently.
#
# USAGE:
#   python3 re-register.py
#   python3 re-register.py --dry-run   # just list what would be registered
# ==============================================================================

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

try:
    import boto3
    import requests
    from botocore.client import Config
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Run: pip install boto3 python-dotenv requests")
    sys.exit(1)

# ── Load same .env as the ingestor ──────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
ENV_FILE = SCRIPT_DIR / ".env"
if ENV_FILE.exists():
    load_dotenv(dotenv_path=ENV_FILE)
else:
    load_dotenv()

BACKEND_URL   = os.getenv("BACKEND_URL", "https://api.innovatex.dev").rstrip("/")
WORKER_API_KEY = os.getenv("WORKER_API_KEY", "")

if not WORKER_API_KEY:
    print("ERROR: WORKER_API_KEY not set in .env")
    sys.exit(1)

HEADERS = {"X-WORKER-KEY": WORKER_API_KEY}


def fetch_storage_credentials() -> tuple[dict, str]:
    """
    Fetch a task from the backend to get R2 credentials + venue slug.
    The ingestor always does this — we reuse the same endpoint.
    Works even when status is 'idle'; we only need the storage block.
    """
    print(f"Fetching credentials from {BACKEND_URL}/api/ingestion/task ...")
    resp = requests.get(f"{BACKEND_URL}/api/ingestion/task", headers=HEADERS, timeout=30)

    if resp.status_code == 401:
        print("ERROR: Invalid WORKER_API_KEY")
        sys.exit(1)
    if not resp.ok:
        print(f"ERROR: Task fetch failed ({resp.status_code}): {resp.text}")
        sys.exit(1)

    data = resp.json()
    storage = data.get("storage")
    venue_slug = data.get("venueSlug", "")

    if not storage:
        print("ERROR: Backend returned no storage block.")
        print("  This can happen if the venue has no R2 config set.")
        print(f"  Raw response: {data}")
        sys.exit(1)

    print(f"  Venue:  {venue_slug}")
    print(f"  Bucket: {storage['bucket']}")
    print(f"  Endpoint: {storage['endpoint']}")
    return storage, venue_slug


def get_r2_client(storage: dict):
    endpoint = storage["endpoint"].strip()
    if not endpoint.startswith("http"):
        use_ssl = storage.get("useSsl", True)
        scheme = "https" if use_ssl else "http"
        endpoint = f"{scheme}://{endpoint}"

    return boto3.client(
        "s3",
        endpoint_url=endpoint.rstrip("/"),
        aws_access_key_id=storage["accessKey"],
        aws_secret_access_key=storage["secretKey"],
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="auto",
    )


def list_r2_match_times(client, bucket: str, venue_slug: str) -> list[str]:
    """List all match_time folders under venue_slug/ in R2."""
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=f"{venue_slug}/", Delimiter="/")

    match_times = []
    for page in pages:
        for prefix in page.get("CommonPrefixes", []):
            # "spondias-salax/2026-06-12_00-00/" → "2026-06-12_00-00"
            folder = prefix["Prefix"].rstrip("/").split("/")[-1]
            if folder:
                match_times.append(folder)

    return sorted(match_times)


def register(match_time: str, venue_slug: str, storage: dict, dry_run: bool) -> str:
    minio_path = f"{venue_slug}/{match_time}/"

    if dry_run:
        print(f"  [DRY RUN] would register {match_time}  →  {minio_path}")
        return "dry_run"

    resp = requests.post(
        f"{BACKEND_URL}/api/ingestion/register",
        json={"matchTime": match_time, "minioPath": minio_path},
        headers=HEADERS,
        timeout=30,
    )

    if resp.status_code in (200, 201):
        return "registered"
    elif resp.status_code == 409:
        return "already_registered"
    else:
        return f"error_{resp.status_code}: {resp.text}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Re-register Saha Live matches from R2.")
    parser.add_argument("--dry-run", action="store_true", help="List matches without registering")
    parser.add_argument("--venue", help="Override venue slug (default: from task response)")
    args = parser.parse_args()

    storage, venue_slug = fetch_storage_credentials()

    if args.venue:
        venue_slug = args.venue
        print(f"  Venue override: {venue_slug}")

    client   = get_r2_client(storage)
    bucket   = storage["bucket"]

    print(f"\nListing R2 folders for venue '{venue_slug}' in bucket '{bucket}' ...")
    match_times = list_r2_match_times(client, bucket, venue_slug)

    if not match_times:
        print("  No folders found. Check venue slug or R2 bucket.")
        return

    print(f"  Found {len(match_times)} folder(s):\n")

    results = {"registered": 0, "already_registered": 0, "error": 0, "dry_run": 0}

    for mt in match_times:
        status = register(mt, venue_slug, storage, dry_run=args.dry_run)

        if status == "registered":
            print(f"  ✅  {mt}  →  registered")
            results["registered"] += 1
        elif status == "already_registered":
            print(f"  ⚠️   {mt}  →  already registered (skip)")
            results["already_registered"] += 1
        elif status == "dry_run":
            results["dry_run"] += 1
        else:
            print(f"  ❌  {mt}  →  {status}")
            results["error"] += 1

    print(f"\nDone.")
    if args.dry_run:
        print(f"  Would register: {len(match_times)} match(es) (dry run, nothing sent)")
    else:
        print(f"  Registered:          {results['registered']}")
        print(f"  Already in DB:       {results['already_registered']}")
        print(f"  Errors:              {results['error']}")


if __name__ == "__main__":
    main()
