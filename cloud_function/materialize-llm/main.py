# main.py (materialize-llm)
import csv
import json
import os
import re
from datetime import datetime, timezone
from typing import Dict

from flask import Request, jsonify
from google.cloud import storage

# -------------------- ENV --------------------
BUCKET_NAME = os.getenv("GCS_BUCKET")                      
LLM_PREFIX  = os.getenv("LLM_PREFIX", "extractor-llm")    # root prefix
storage_client = storage.Client()

RUN_ID_ISO_RE   = re.compile(r"^\d{8}T\d{6}Z$")
RUN_ID_PLAIN_RE = re.compile(r"^\d{14}$")

# -------------------- HELPERS --------------------
def _list_run_ids(bucket: str, llm_prefix: str) -> list[str]:
    it = storage_client.list_blobs(bucket, prefix=f"{llm_prefix}/", delimiter="/")
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

def _jsonl_llm_records(bucket: str, llm_prefix: str, run_id: str):
    b = storage_client.bucket(bucket)
    prefix = f"{llm_prefix}/run_id={run_id}/jsonl_llm/"  # << use new folder
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

# -------------------- MATERIALIZE LLM --------------------
def materialize_llm_http(request: Request):
    """
    Aggregate all LLM JSONL outputs into one CSV (including new fields).
    """
    try:
        if not BUCKET_NAME:
            return jsonify({"ok": False, "error": "missing GCS_BUCKET env"}), 500

        request_json = request.get_json(silent=True) or {}
        run_id_param = request.args.get("run_id") or request_json.get("run_id")
        run_ids = [run_id_param] if run_id_param else _list_run_ids(BUCKET_NAME, LLM_PREFIX)

        if not run_ids:
            return jsonify({"ok": False, "error": f"no runs found under {LLM_PREFIX}/"}), 200

        latest_by_post: Dict[str, Dict] = {}
        skipped_records = 0
        written_records = 0

        for rid in run_ids:
            for rec in _jsonl_llm_records(BUCKET_NAME, LLM_PREFIX, rid):
                pid = rec.get("post_id")
                if not pid:
                    skipped_records += 1
                    continue
                prev = latest_by_post.get(pid)
                if (prev is None) or (_run_id_to_dt(rec["run_id"]) > _run_id_to_dt(prev["run_id"])):
                    latest_by_post[pid] = rec

        records = list(latest_by_post.values())
        if not records:
            return jsonify({"ok": True, "message": "no records found", "runs_scanned": len(run_ids)}), 200

        # --- Collect all keys across all records (dynamic columns) ---
        columns = set()
        for r in records:
            columns.update(r.keys())
        columns = sorted(columns)

        final_key = f"{LLM_PREFIX}/datasets/listings_master-llm.csv"
        with _open_gcs_text_writer(BUCKET_NAME, final_key) as out:
            writer = csv.DictWriter(out, fieldnames=columns)
            writer.writeheader()
            for r in records:
                writer.writerow(r)
                written_records += 1

        return jsonify({
            "ok": True,
            "runs_scanned": len(run_ids),
            "unique_listings": len(records),
            "columns": columns,
            "skipped_records": skipped_records,
            "written_records": written_records,
            "output_csv": f"gs://{BUCKET_NAME}/{final_key}"
        })

    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500