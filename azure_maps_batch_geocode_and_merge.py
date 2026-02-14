#!/usr/bin/env python3
"""
Azure Maps Batch Geocoding for Locations.xlsx

Reads the 'Locations' sheet, uses the 'FullAddress' column to submit batch geocoding
jobs to Azure Maps Search Address Batch (async) API, polls for completion, and writes
Lat/Long, MatchStatus, and Confidence back into the workbook.

Usage (GitHub Actions):
  1) Store your Azure Maps Primary Key as a repo secret named AZURE_MAPS_KEY.
  2) Put this script and 'Locations_geocode_ready.xlsx' at the repo root.
  3) Run the workflow; this script is invoked with:
       python azure_maps_batch_geocode_and_merge.py

Notes:
  - The Azure Maps async batch flow is: POST -> 202 + Location header -> poll the
    Location URL until results are available (often returned directly as batchItems).
  - We check both 'Location' and 'Operation-Location' headers for robustness.
  - We honor 'Retry-After' headers while polling and allow up to 30 minutes.
  - We merge query params into the returned Location URL to avoid duplicates.
  - BATCH_SIZE is 100 by default to reduce throttling and improve reliability.
"""

import os
import time
import json
import math
import copy
import pandas as pd
import datetime as dt
import requests
from pathlib import Path
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

# -------------------------
# Configuration constants
# -------------------------
INPUT_FILE  = 'Locations_geocode_ready.xlsx'
SHEET_NAME  = 'Locations'
BATCH_SIZE  = 100           # submit in smaller async jobs for reliability
COUNTRY_SET = 'US'
API_VERSION = '1.0'
BASE_URL    = 'https://atlas.microsoft.com/search/address/batch/json'

# Columns
ADDR_COL = 'FullAddress'
LAT_COL  = 'Lat'
LON_COL  = 'Long'
STATUS_COL = 'MatchStatus'
CONF_COL   = 'Confidence'


# -------------------------
# Helpers
# -------------------------
def load_key():
    key = os.environ.get('AZURE_MAPS_KEY')
    if not key:
        raise RuntimeError('AZURE_MAPS_KEY environment variable is not set.')
    return key


def load_locations():
    df = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME, engine='openpyxl')
    if ADDR_COL not in df.columns:
        raise ValueError(f"Missing '{ADDR_COL}' column in sheet '{SHEET_NAME}'.")
    # Ensure result columns exist
    for col in [LAT_COL, LON_COL, STATUS_COL, CONF_COL]:
        if col not in df.columns:
            df[col] = ''
    df['_rowid'] = range(len(df))
    to_geocode = df[df[ADDR_COL].astype(str).str.strip() != ''].copy()
    return df, to_geocode


def build_batch_requests(rows):
    """Build Azure Maps Search Address batchItems payload."""
    reqs = []
    for _, r in rows.iterrows():
        query = str(r[ADDR_COL]).strip()
        reqs.append({
            'query': query,
            'countrySet': COUNTRY_SET
        })
    return reqs


def submit_batch(session, key, reqs):
    """
    Submit async batch. Azure Maps returns 202 and the poll URL in the Location header
    (some services may use Operation-Location; we check both).
    """
    params = {'api-version': API_VERSION, 'subscription-key': key}
    payload = {'batchItems': reqs}
    resp = session.post(BASE_URL, params=params, json=payload, timeout=60)
    resp.raise_for_status()

    op_loc = (
        resp.headers.get('location') or resp.headers.get('Location') or
        resp.headers.get('operation-location') or resp.headers.get('Operation-Location')
    )
    if not op_loc:
        raise RuntimeError('Missing Location/Operation-Location header in response.')
    return op_loc


def _merge_params_into_url(url, extra_params):
    """
    Merge extra_params into url's querystring without duplicating keys.
    Returns a new URL.
    """
    parsed = urlparse(url)
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for k, v in extra_params.items():
        if k not in q:
            q[k] = v
    new_query = urlencode(q)
    return urlunparse(parsed._replace(query=new_query))


def poll_batch(session, key, op_loc, poll_interval=2, timeout_sec=1800):
    """
    Poll the async batch URL until results are available or timeout.
    Behavior:
      - 202 -> still running; sleep (honor Retry-After) then continue
      - 200 -> if 'batchItems' in body, results are ready (return);
               else check 'summary.state' or 'status' and keep polling for running states
    """
    start = time.time()
    while True:
        # Build a safe poll URL that includes any missing params (no duplicates).
        poll_url = _merge_params_into_url(op_loc, {
            'api-version': API_VERSION,
            'subscription-key': key
        })

        r = session.get(poll_url, timeout=60)

        # Still running; service returns 202 with (often) no body.
        if r.status_code == 202:
            retry_after = r.headers.get('Retry-After')
            sleep_s = max(poll_interval, int(retry_after) if retry_after and retry_after.isdigit() else poll_interval)
            print(f"Batch still running (202). Sleeping {sleep_s}s...")
            time.sleep(min(sleep_s, 15))
            if time.time() - start > timeout_sec:
                raise TimeoutError('Polling timed out for batch job (still 202).')
            continue

        # Completed or error
        r.raise_for_status()

        # Attempt to parse JSON; if not JSON, short sleep and retry
        try:
            data = r.json()
        except Exception:
            time.sleep(3)
            if time.time() - start > timeout_sec:
                raise TimeoutError('Polling timed out for batch job (non-JSON response).')
            continue

        # If results are ready, many responses return 'batchItems' directly.
        if isinstance(data, dict) and 'batchItems' in data:
            print("Batch results available (batchItems present).")
            return data

        # Otherwise check state if provided
        state = (data.get('summary', {}) or {}).get('state') or data.get('status')
        print(f"Batch state: {state}")

        if state in ('Running', 'Pending', 'InProgress'):
            retry_after = r.headers.get('Retry-After')
            sleep_s = max(poll_interval, int(retry_after) if retry_after and retry_after.isdigit() else poll_interval)
            time.sleep(min(sleep_s, 15))
            if time.time() - start > timeout_sec:
                raise TimeoutError('Polling timed out for batch job.')
            continue

        if state in ('Succeeded', 'Completed'):
            return data

        if state in ('Failed', 'Error'):
            # Truncate to avoid massive logs
            raise RuntimeError(f"Batch failed: {str(data)[:500]}")

        # Unknown/empty state—short sleep and retry
        time.sleep(3)
        if time.time() - start > timeout_sec:
            raise TimeoutError('Polling timed out for batch job (unknown state).')


def parse_results(batch_result):
    """
    Extract best match per request from Azure Maps response.
    Each item has 'response' with 'results' array -> take [0] for best.
    """
    out = []
    items = batch_result.get('batchItems') or []
    for item in items:
        resp = item.get('response') or {}
        reslist = resp.get('results') or []
        if reslist:
            best = reslist[0]
            pos = best.get('position', {})
            out.append({
                'lat': pos.get('lat'),
                'lon': pos.get('lon'),
                'status': best.get('type') or best.get('entityType') or 'Matched',
                'confidence': best.get('score')
            })
        else:
            out.append({'lat': None, 'lon': None, 'status': 'No Match', 'confidence': None})
    return out


def backup_file(path):
    p = Path(path)
    ts = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup = p.with_name(p.stem + f'.backup_{ts}' + p.suffix)
    backup.write_bytes(p.read_bytes())
    return backup


# -------------------------
# Main flow
# -------------------------
def main():
    key = load_key()
    df, to_geocode = load_locations()
    if to_geocode.empty:
        print('No addresses to geocode.')
        return

    session = requests.Session()

    n = len(to_geocode)
    n_batches = math.ceil(n / BATCH_SIZE)
    print(f'Submitting {n} addresses in {n_batches} batch(es)...')

    all_results = []
    for i in range(n_batches):
        start = i * BATCH_SIZE
        end   = min((i + 1) * BATCH_SIZE, n)
        chunk = to_geocode.iloc[start:end]

        reqs = build_batch_requests(chunk)
        op_loc = submit_batch(session, key, reqs)
        print(f'Batch {i+1}/{n_batches} submitted. Polling...')

        batch_json = poll_batch(session, key, op_loc)
        results = parse_results(batch_json)

        if len(results) != len(chunk):
            raise RuntimeError('Result length mismatch for batch.')

        all_results.extend(results)
        print(f'Batch {i+1}/{n_batches} completed.')

    # Map results back to rows
    geocoded = copy.deepcopy(to_geocode).reset_index(drop=True)
    for idx, r in enumerate(all_results):
        geocoded.at[idx, LAT_COL]    = r['lat']
        geocoded.at[idx, LON_COL]    = r['lon']
        geocoded.at[idx, STATUS_COL] = r['status']
        geocoded.at[idx, CONF_COL]   = r['confidence']

    merged = df.merge(
        geocoded[['_rowid', LAT_COL, LON_COL, STATUS_COL, CONF_COL]],
        on='_rowid', how='left', suffixes=('', '_new')
    )

    for col in [LAT_COL, LON_COL, STATUS_COL, CONF_COL]:
        merged[col] = merged[col].where(merged[col].notna(), merged[f'{col}_new'])
        merged.drop(columns=[f'{col}_new'], inplace=True)

    merged.drop(columns=['_rowid'], inplace=True)

    # Backup then write in place
    backup = backup_file(INPUT_FILE)
    print(f'Backup written: {backup}')

    with pd.ExcelWriter(INPUT_FILE, engine='openpyxl', mode='w') as writer:
        merged.to_excel(writer, sheet_name=SHEET_NAME, index=False)

    print(f'Updated workbook written: {INPUT_FILE}')


if __name__ == '__main__':
    main()
