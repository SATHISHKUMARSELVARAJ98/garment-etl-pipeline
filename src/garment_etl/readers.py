from __future__ import annotations
import csv, json
from pathlib import Path
from typing import Iterator

SUPPORTED = {".csv", ".jsonl", ".ndjson", ".parquet"}

def detect_format(path: Path, requested="auto") -> str:
    if requested != "auto":
        return requested
    ext = path.suffix.lower()
    if ext == ".csv": return "csv"
    if ext in {".jsonl", ".ndjson"}: return "jsonl"
    if ext == ".parquet": return "parquet"
    raise ValueError(f"Unsupported file extension: {ext}")

def discover_sources(source: Path, preferred_name: str | None = None) -> list[Path]:
    if source.is_file(): return [source]
    if not source.is_dir(): raise FileNotFoundError(f"Source path does not exist: {source}")
    preferred = source / preferred_name if preferred_name else None
    if preferred and preferred.exists(): return [preferred]
    return sorted(p for p in source.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED)

def read_records(path: Path, source_format="auto", batch_size=1000) -> Iterator[dict]:
    fmt = detect_format(path, source_format)
    if fmt == "csv":
        with path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames: raise ValueError(f"CSV file has no header: {path}")
            yield from (dict(row) for row in reader)
    elif fmt == "jsonl":
        with path.open(encoding="utf-8") as f:
            for line_no, line in enumerate(f, 1):
                if not line.strip(): continue
                try: row = json.loads(line)
                except json.JSONDecodeError as exc: raise ValueError(f"Invalid JSONL line {line_no}: {exc}") from exc
                if not isinstance(row, dict): raise ValueError(f"JSONL line {line_no} must be an object")
                yield row
    elif fmt == "parquet":
        import pyarrow.parquet as pq
        for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_size):
            yield from batch.to_pylist()
    else:
        raise ValueError(f"Unsupported format: {fmt}")
