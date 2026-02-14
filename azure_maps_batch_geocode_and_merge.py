#!/usr/bin/env python3
"""
Azure Maps Batch Geocoding for Locations.xlsx

Reads the 'Locations' sheet, uses the 'FullAddress' column to submit batch geocoding
jobs to Azure Maps Search Batch API, polls for completion, and writes Lat/Long,
MatchStatus, and Confidence back into the workbook.

Usage:
  1) Provide AZURE_MAPS_KEY as an environment secret in your GitHub Action
     (Settings → Secrets and variables → Actions → New repository secret).
  2) Place this script at the repo root alongside 'Locations_geocode_ready.xlsx'.
  3) The workflow runs: python azure_maps_batch_geocode_and_merge.py

Notes:
  - BATCH_SIZE default (500) balances throughput and throttling.
  - Restricts search to countrySet=US to improve precision for your data.
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

INPUT_FILE = 'Locations_geocode_ready.xlsx'
SHEET_NAME = 'Locations'
BATCH_SIZE = 500
COUNTRY_SET = 'US'
API_VERSION = '1.0'
BASE_URL = 'https://atlas.microsoft.com/search/address/batch/json'

# Columns
ADDR_COL = 'FullAddress'
LAT_COL = 'Lat'
LON_COL = 'Long'
STATUS_COL = 'MatchStatus'
CONF_COL = 'Confidence'


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
    reqs = []
    for _, r in rows.iterrows():
        query = str(r[ADDR_COL]).strip()
        reqs.append({
            'query': query,
            'countrySet': COUNTRY_SET
        })
    return reqs


def submit_batch(session, key, reqs):
    params = {
        'api-version': API_VERSION,
        'subscription-key': key
    }
    payload = {'batchItems': reqs}
    resp = session.post(BASE_URL, params=params, json=payload, timeout=60)
    resp.raise_for_status()
    # Azure Maps returns the poll URL in the *Location* header for async batches.
    # Fall back to Operation-Location just in case.
    op_loc = (
        resp.headers.get('location') or resp.headers.get('Location') or
        resp.headers.get('operation-location') or resp.headers.get('Operation-Location')
    )
    if not op_loc:
        # Optional: print headers for quick debugging if ever needed
        # print(f"Status={resp.status_code}, Headers={dict(resp.headers)}")
        raise RuntimeError('Missing Location/Operation-Location header in response.')
    return op_loc


def poll_batch(session, key, op_loc, poll_interval=2, timeout_sec=300):
    start = time.time()
    while True:
        params = {'api-version': API_VERSION, 'subscription-key': key}
        r = session.get(op_loc, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        status = data.get('summary', {}).get('state') or data.get('status')
        if status in ('Succeeded', 'Completed'):
            return data
        if status in ('Failed', 'Error'):
            raise RuntimeError(f'Batch failed: {json.dumps(data)[:500]}')
        if time.time() - start > timeout_sec:
            raise TimeoutError('Polling timed out for batch job.')
        time.sleep(poll_interval)


def parse_results(batch_result):
    results = []
    items = batch_result.get('batchItems') or []
    for item in items:
        resp = item.get('response') or {}
        reslist = resp.get('results') or []
        if reslist:
            best = reslist[0]
            pos = best.get('position', {})
            lat = pos.get('lat')
            lon = pos.get('lon')
            score = best.get('score')
            match = best.get('type') or best.get('entityType') or 'Matched'
            results.append({'lat': lat, 'lon': lon, 'status': match, 'confidence': score})
        else:
            results.append({'lat': None, 'lon': None, 'status': 'No Match', 'confidence': None})
    return results


def backup_file(path):
    p = Path(path)
    ts = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup = p.with_name(p.stem + f'.backup_{ts}' + p.suffix)
    backup.write_bytes(p.read_bytes())
    return backup


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
        end = min((i + 1) * BATCH_SIZE, n)
        chunk = to_geocode.iloc[start:end]
        reqs = build_batch_requests(chunk)
        op_loc = submit_batch(session, key, reqs)
        print(f'Batch {i + 1}/{n_batches} submitted. Polling...')
        batch_json = poll_batch(session, key, op_loc)
        results = parse_results(batch_json)
        if len(results) != len(chunk):
            raise RuntimeError('Result length mismatch for batch.')
        all_results.extend(results)
        print(f'Batch {i + 1}/{n_batches} completed.')

    geocoded = copy.deepcopy(to_geocode).reset_index(drop=True)
    for idx, r in enumerate(all_results):
        geocoded.at[idx, LAT_COL] = r['lat']
        geocoded.at[idx, LON_COL] = r['lon']
        geocoded.at[idx, STATUS_COL] = r['status']
        geocoded.at[idx, CONF_COL] = r['confidence']

    merged = df.merge(
        geocoded[['_rowid', LAT_COL, LON_COL, STATUS_COL, CONF_COL]],
        on='_rowid', how='left', suffixes=('', '_new')
    )

    for col in [LAT_COL, LON_COL, STATUS_COL, CONF_COL]:
        merged[col] = merged[col].where(merged[col].notna(), merged[f'{col}_new'])
        merged.drop(columns=[f'{col}_new'], inplace=True)

    merged.drop(columns=['_rowid'], inplace=True)

    backup = backup_file(INPUT_FILE)
    print(f'Backup written: {backup}')

    with pd.ExcelWriter(INPUT_FILE, engine='openpyxl', mode='w') as writer:
        merged.to_excel(writer, sheet_name=SHEET_NAME, index=False)

    print(f'Updated workbook written: {INPUT_FILE}')


if __name__ == '__main__':
    main()
