import csv, hashlib, json, logging
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import text
from garment_etl.readers import discover_sources, read_records
from garment_etl.validation import normalize_order
from garment_etl.record_tracking import classify_record

def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024*1024), b""): digest.update(chunk)
    return digest.hexdigest()

def sha256_record(payload):
    canonical = json.dumps(
        json_safe(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

def json_safe(value):
    if isinstance(value, dict): return {str(k): json_safe(v) for k,v in value.items()}
    if isinstance(value, (list,tuple)): return [json_safe(v) for v in value]
    if hasattr(value, "isoformat"): return value.isoformat()
    if value is None or isinstance(value,(str,int,float,bool)): return value
    return str(value)

def run_pipeline(config, engine, logger, source=None, force_baseline=False):
    selected = source or (config.source_dir / config.source_file)
    sources = discover_sources(selected, config.source_file if source is None and selected.is_dir() else None)
    if not sources: raise FileNotFoundError(f"No supported source files found: {selected}")
    s, a = config.schemas, config.schemas["audit"]
    with engine.begin() as conn:
        run_id = conn.execute(text(f'INSERT INTO "{a}".pipeline_run(pipeline_name,status) VALUES (:n,:s) RETURNING run_id'),
                              {"n":config.name,"s":"RUNNING"}).scalar_one()
    totals = dict(
            run_id=run_id,
            files_seen=len(sources),
            files_skipped=0,
            files_succeeded=0,
            files_failed=0,
            rows_read=0,
            rows_accepted=0,
            rows_rejected=0,
            new_records=0,
            changed_records=0,
            unchanged_records=0,
        )
    for path in sources:
        name = path.name

        try:
            digest = sha256_file(path)
        except Exception as exc:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        f"""
                        UPDATE "{a}".pipeline_run
                        SET status = 'FAILED',
                            finished_at = :finished_at,
                            error_message = :error_message
                        WHERE run_id = :run_id
                        """
                    ),
                    {
                        "finished_at": datetime.now(timezone.utc),
                        "error_message": (f"Failed to hash source file '{path}': {exc}")[:4000],
                        "run_id": run_id,
                    },
                )

            raise
        try:
            if config.skip_completed_files and not force_baseline:
                with engine.connect() as conn:
                    done = conn.execute(text(f'SELECT 1 FROM "{a}".file_load WHERE source_name=:n AND source_sha256=:h AND status=:status LIMIT 1'),
                                        {"n":name,"h":digest,"status":"SUCCEEDED"}).first()
                if done:
                    totals["files_skipped"] += 1; logger.info("Skipping completed file %s", path); continue
            with engine.begin() as conn:
                file_id = conn.execute(text(f'''
                    INSERT INTO "{a}".file_load(run_id,source_path,source_name,source_sha256,status)
                    VALUES (:r,:p,:n,:h,'RUNNING')
                    ON CONFLICT(source_sha256,source_name) DO UPDATE SET run_id=EXCLUDED.run_id,
                    source_path=EXCLUDED.source_path,status='RUNNING',started_at=now(),finished_at=NULL,error_message=NULL
                    RETURNING file_load_id'''), {"r":run_id,"p":str(path.resolve()),"n":name,"h":digest}).scalar_one()
            read = accepted = rejected_count = 0
            rejects = []
            new_records = 0
            changed_records = 0
            unchanged_records = 0
            # One transaction per file keeps raw, reject, staging, integration, and warehouse changes consistent.
            with engine.begin() as conn:
                for rownum, payload in enumerate(read_records(path, config.source_format, config.batch_size), 1):
                    read += 1
                    safe = json_safe(payload)

                    conn.execute(text(f'''INSERT INTO "{s['raw']}".source_order(file_load_id,row_number,source_payload)
                        VALUES (:f,:r,CAST(:p AS JSONB)) ON CONFLICT(file_load_id,row_number) DO NOTHING'''),
                        {"f":file_id,"r":rownum,"p":json.dumps(safe,ensure_ascii=False)})
                    normalized, errors = normalize_order(payload, config.allowed_currencies, config.allowed_statuses,
                                                         config.require_order_date_before_receive)
                    if errors:
                        rejected_count += 1; rejects.append((rownum,errors,safe))
                        conn.execute(text(f'''
                                INSERT INTO "{a}".rejected_record
                                    (file_load_id, row_number, source_payload, error_reasons)
                                VALUES (:f, :r, CAST(:p AS JSONB), CAST(:e AS JSONB))
                                ON CONFLICT (file_load_id, row_number) DO NOTHING
                                '''),
                            {"f":file_id,"r":rownum,"p":json.dumps(safe,ensure_ascii=False),"e":json.dumps(errors)})
                        continue
                    accepted += 1
                    
                    record_key = str(safe.get("order_id", "")).strip()
                    record_hash = sha256_record(safe)

                    previous_hash = conn.execute(
                        text(f'''
                            SELECT record_hash
                            FROM "{a}".source_record_state
                            WHERE source_name = :source_name
                            AND record_key = :record_key
                        '''),
                        {
                            "source_name": name,
                            "record_key": record_key,
                        },
                    ).scalar_one_or_none()

                    record_status = classify_record(previous_hash, record_hash)

                    if record_status == "unchanged":
                        unchanged_records += 1

                        conn.execute(
                            text(f'''
                                UPDATE "{a}".source_record_state
                                SET last_seen_at = now()
                                WHERE source_name = :source_name
                                AND record_key = :record_key
                            '''),
                            {
                                "source_name": name,
                                "record_key": record_key,
                            },
                        )
                        continue

                    if record_status == "new":
                        new_records += 1
                    else:
                        changed_records += 1
    
                    params = normalized | {"source_file_load_id":file_id}
                    cols = "order_id,style_id,supplier_id,order_date,receive_date,delivery_date,quantity,unit_price,total_amount,currency,status,source_file_load_id"
                    vals = ",".join(":"+c for c in cols.split(","))
                    common = "style_id=EXCLUDED.style_id,supplier_id=EXCLUDED.supplier_id,order_date=EXCLUDED.order_date,receive_date=EXCLUDED.receive_date,delivery_date=EXCLUDED.delivery_date,quantity=EXCLUDED.quantity,unit_price=EXCLUDED.unit_price,total_amount=EXCLUDED.total_amount,currency=EXCLUDED.currency,status=EXCLUDED.status,source_file_load_id=EXCLUDED.source_file_load_id"
                    
                    conn.execute(
                        text(f'''
                            INSERT INTO "{a}".source_record_state (
                                source_name, record_key, record_hash, last_seen_at
                            )
                            VALUES (
                                :source_name, :record_key, :record_hash, now()
                            )
                            ON CONFLICT (source_name, record_key)
                            DO UPDATE SET
                                record_hash = EXCLUDED.record_hash,
                                last_seen_at = now()
                        '''),
                        {
                            "source_name": name,
                            "record_key": record_key,
                            "record_hash": record_hash,
                        },
                    )
                                            
                    for schema, table, stamp in [(s["staging"],"garment_order","staged_at"),(s["integration"],"garment_order","updated_at"),(s["warehouse"],"fact_garment_order","loaded_at")]:

                        
                        # Avoid rewriting created_at on conflict; updated timestamp is maintained for integration.
                        sql = f'INSERT INTO "{schema}"."{table}" ({cols}) VALUES ({vals}) ON CONFLICT(order_id) DO UPDATE SET {common},{stamp}=now()'
                        conn.execute(text(sql), params)
                conn.execute(text(f'''UPDATE "{a}".file_load SET status='SUCCEEDED',finished_at=now(),
                    rows_read=:r,rows_accepted=:ok,rows_rejected=:bad WHERE file_load_id=:f'''),
                    {"r":read,"ok":accepted,"bad":rejected_count,"f":file_id})
            if rejects:
                config.rejected_dir.mkdir(parents=True, exist_ok=True)
                out = config.rejected_dir / f"{path.stem}_{digest[:10]}_rejected.csv"
                with out.open("w",newline="",encoding="utf-8") as f:
                    writer=csv.DictWriter(f,fieldnames=["row_number","error_reasons","source_payload"]); writer.writeheader()
                    for rn, errs, payload in rejects:
                        writer.writerow({"row_number":rn,"error_reasons":" | ".join(errs),"source_payload":json.dumps(payload,ensure_ascii=False)})
            totals["files_succeeded"] += 1
            for key,value in [("rows_read",read),("rows_accepted",accepted),("rows_rejected",rejected_count)]: 
                totals[key]+=value
            totals["new_records"] += new_records
            totals["changed_records"] += changed_records
            totals["unchanged_records"] += unchanged_records
            logger.info(
                "Completed %s: read=%s accepted=%s rejected=%s "
                "new=%s changed=%s unchanged=%s",
                name,
                read,
                accepted,
                rejected_count,
                new_records,
                changed_records,
                unchanged_records,
            )
        except Exception as exc:
            totals["files_failed"] += 1
            logger.exception("Failed processing %s", path)

            try:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            f"""
                            UPDATE "{a}".file_load
                            SET status = 'FAILED',
                                finished_at = now(),
                                error_message = :error_message
                            WHERE source_name = :source_name
                              AND source_sha256 = :source_sha256
                            """
                        ),
                        {
                            "error_message": str(exc)[:4000],
                            "source_name": name,
                            "source_sha256": digest,
                        },
                    )

                    conn.execute(
                        text(
                            f"""
                            INSERT INTO "{a}".error_log
                                (run_id, error_type, error_message)
                            VALUES (:run_id, :error_type, :error_message)
                            """
                        ),
                        {
                            "run_id": run_id,
                            "error_type": type(exc).__name__,
                            "error_message": str(exc)[:4000],
                        },
                    )

            except Exception as audit_exc:
                logger.exception("Failed writing error audit records")

                # Independently mark the file load as FAILED.
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text(
                                f"""
                                UPDATE "{a}".file_load
                                SET status = 'FAILED',
                                    finished_at = now(),
                                    error_message = :error_message
                                WHERE source_name = :source_name
                                  AND source_sha256 = :source_sha256
                                  AND run_id = :run_id
                                """
                            ),
                            {
                                "error_message": (
                                    f"Processing error: {exc}; "
                                    f"Audit error: {audit_exc}"
                                )[:4000],
                                "source_name": name,
                                "source_sha256": digest,
                                "run_id": run_id,
                            },
                        )
                except Exception:
                    logger.exception(
                        "Could not update file_load after audit failure"
                    )

                # Keep the existing pipeline_run failure update.
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text(
                                f"""
                                UPDATE "{a}".pipeline_run
                                SET status = 'FAILED',
                                    finished_at = :finished_at,
                                    error_message = :error_message,
                                    files_failed = :files_failed
                                WHERE run_id = :run_id
                                """
                            ),
                            {
                                "finished_at": datetime.now(timezone.utc),
                                "error_message": (
                                    f"Processing error: {exc}; "
                                    f"Audit error: {audit_exc}"
                                )[:4000],
                                "files_failed": totals["files_failed"],
                                "run_id": run_id,
                            },
                        )
                except Exception:
                    logger.exception(
                        "Could not update pipeline_run after audit failure"
                    )
                    raise

                raise

    status = "FAILED" if totals["files_failed"] else "SUCCEEDED"
    
    with engine.begin() as conn:
        conn.execute(
            text(f'''
                UPDATE "{a}".pipeline_run
                SET finished_at = :t,
                    status = :s,
                    files_seen = :fs,
                    files_skipped = :sk,
                    files_succeeded = :ok,
                    files_failed = :bad,
                    rows_read = :r,
                    rows_accepted = :ac,
                    rows_rejected = :rej,
                    new_records = :new,
                    changed_records = :changed,
                    unchanged_records = :unchanged
                WHERE run_id = :id
            '''),
            {
                "t": datetime.now(timezone.utc),
                "s": status,
                "fs": totals["files_seen"],
                "sk": totals["files_skipped"],
                "ok": totals["files_succeeded"],
                "bad": totals["files_failed"],
                "r": totals["rows_read"],
                "ac": totals["rows_accepted"],
                "rej": totals["rows_rejected"],
                "new": totals["new_records"],
                "changed": totals["changed_records"],
                "unchanged": totals["unchanged_records"],
                "id": run_id,
            },
        )

    return totals | {"status":status}