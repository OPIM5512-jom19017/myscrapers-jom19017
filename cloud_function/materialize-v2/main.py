# main.py (materialize-v2)
import csv
import json
import os
import re
from datetime import datetime, timezone
from typing import Dict, Iterable

from flask import Request, jsonify
from google.cloud import storage

# -------------------- ENV --------------------
BUCKET_NAME        = os.getenv("GCS_BUCKET")                      
STRUCTURED_PREFIX  = os.getenv("STRUCTURED_PREFIX", "structured") 

storage_client = storage.Client()

RUN_ID_ISO_RE   = re.compile(r"^\d{8}T\d{6}Z$")
RUN_ID_PLAIN_RE = re.compile(r"^\d{14}$")

# -------------------- Parse Listing --------------------
# Re-use your updated parse_listing with all new fields
PRICE_RE = re.compile(r"\$\s*([\d,]+)")
YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
MAKE_MODEL_RE = re.compile(r"\b(?!(?:Contact Information|Email|Phone))([A-Z][a-zA-Z]+)[\s-]+([A-Z][a-zA-Z0-9]+)\b")
DOORS_RE = re.compile(r"(\d)[- ]?door", re.I)
FUEL_RE = re.compile(r"\b(diesel|gas|gasoline|electric|hybrid)\b", re.I)
TRANS_RE = re.compile(r"\b(automatic|manual|cvt|cvT)\b", re.I)
TRUCK_RE = re.compile(r"\b(truck|pickup|towing)\b", re.I)

def parse_listing(text: str) -> dict:
    d = {}
    # Price
    m = PRICE_RE.search(text)
    if m:
        try:
            d["price"] = int(m.group(1).replace(",", ""))
        except ValueError:
            pass
    # Year
    y = YEAR_RE.search(text)
    if y:
        try:
            d["year"] = int(y.group(0))
        except ValueError:
            pass
    # Make & Model
    mm = MAKE_MODEL_RE.search(text)
    if mm:
        d["make"] = mm.group(1)
        d["model"] = mm.group(2)
    # Mileage
    mi = None
    m1 = re.search(r"(?:mileage|odometer)\s*[:\-]?\s*([\d,]+)", text, re.I)
    if m1:
        try: mi = int(m1.group(1).replace(",", ""))
        except ValueError: mi = None
    if mi is None:
        m2 = re.search(r"(\d+(?:\.\d+)?)\s*k\s*(?:mi|mile|miles)\b", text, re.I)
        if m2:
            try: mi = int(float(m2.group(1)) * 1000)
            except ValueError: mi = None
    if mi is None:
        m3 = re.search(r"(\d{1,3}(?:[,\d]{3})*)\s*(?:mi|mile|miles)\b", text, re.I)
        if m3:
            try: mi = int(re.sub(r"[^\d]", "", m3.group(1)))
            except ValueError: mi = None
    if mi is not None:
        d["mileage"] = mi
    # New fields
    doors = DOORS_RE.search(text)
    if doors:
        try: d["num_doors"] = int(doors.group(1))
        except ValueError: d["num_doors"] = None
    fuel = FUEL_RE.search(text)
    if fuel:
        d["fuel_type"] = fuel.group(1).lower()
    trans = TRANS_RE.search(text)
    if trans:
        d["transmission"] = trans.group(1).lower()
    d["is_truck"] = bool(TRUCK_RE.search(text))
    return d

# -------------------- Materialize --------------------
def _list_run_ids(bucket: str, structured_prefix: str) -> list[str]:
    it = storage_client.list_blobs(bucket, prefix=f"{structured_prefix}/", delimiter="/")
    for _ in it:
        pass
    run_ids = []
    for p in getattr(it, "prefixes", []):
        tail = p.rstrip("/").split("/")[-1]
        if tail.startswith("run_id="):
            rid = tail.split("run_id=", 1)[1]
            if RUN_ID_ISO_RE.match(rid) or RUN_ID_PLAIN_RE.match(rid):
                run_ids.append(rid)
    return sorted(run_ids)

def _jsonl_records_for_run(bucket: str, structured_prefix: str, run_id: str):
    b = storage_client.bucket(bucket)
    prefix = f"{structured_prefix}/run_id={run_id}/jsonl/"
    for blob in b.list_blobs(prefix=prefix):
        if not blob.name.endswith(".jsonl"):
            continue
        line = blob.download_as_text().strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            rec.setdefault("run_id", run_id)
            yield rec
        except Exception:
            continue

def _run_id_to_dt(rid: str) -> datetime:
    if RUN_ID_ISO_RE.match(rid):
        return datetime.strptime(rid, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    if RUN_ID_PLAIN_RE.match(rid):
        return datetime.strptime(rid, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)

def _open_gcs_text_writer(bucket: str, key: str):
    b = storage_client.bucket(bucket)
    blob = b.blob(key)
    return blob.open("w")

def materialize_v2_http(request: Request):
    try:
        if not BUCKET_NAME:
            return jsonify({"ok": False, "error": "missing GCS_BUCKET env"}), 500

        run_ids = _list_run_ids(BUCKET_NAME, STRUCTURED_PREFIX)
        if not run_ids:
            return jsonify({"ok": False, "error": "no runs found"}), 200

        latest_by_post: Dict[str, Dict] = {}
        for rid in run_ids:
            for rec in _jsonl_records_for_run(BUCKET_NAME, STRUCTURED_PREFIX, rid):
                pid = rec.get("post_id")
                if not pid:
                    continue
                prev = latest_by_post.get(pid)
                if (prev is None) or (_run_id_to_dt(rec["run_id"]) > _run_id_to_dt(prev["run_id"])):
                    latest_by_post[pid] = rec

        records = list(latest_by_post.values())

        # --- Re-parse each source_txt to populate new fields ---
        for rec in records:
            source_txt_path = rec.get("source_txt")
            if source_txt_path:
                try:
                    blob = storage_client.bucket(BUCKET_NAME).blob(source_txt_path)
                    raw_text = blob.download_as_text()
                    parsed = parse_listing(raw_text)
                    rec.update(parsed)
                except Exception:
                    pass

        # --- Dynamic columns (all keys across all records) ---
        columns = set()
        for r in records:
            columns.update(r.keys())
        columns = sorted(columns)

        final_key = f"{STRUCTURED_PREFIX}/datasets/listings_master_v2.csv"
        with _open_gcs_text_writer(BUCKET_NAME, final_key) as out:
            writer = csv.DictWriter(out, fieldnames=columns)
            writer.writeheader()
            for r in records:
                writer.writerow(r)

        return jsonify({
            "ok": True,
            "runs_scanned": len(run_ids),
            "unique_listings": len(records),
            "columns": columns,
            "output_csv": f"gs://{BUCKET_NAME}/{final_key}"
        })

    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500