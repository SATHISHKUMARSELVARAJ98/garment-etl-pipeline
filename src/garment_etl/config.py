from __future__ import annotations
import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv
import yaml
from sqlalchemy.engine import URL

@dataclass(frozen=True)
class Config:
    name: str
    batch_size: int
    source_dir: Path
    source_file: str
    source_format: str
    rejected_dir: Path
    log_dir: Path
    skip_completed_files: bool
    create_schemas: bool
    db_url: str
    schemas: dict[str, str]
    allowed_currencies: tuple[str, ...]
    allowed_statuses: tuple[str, ...]
    require_order_date_before_receive: bool
    config_path: Path

def load_config(config_path="config/dev.yaml") -> Config:
    path = Path(config_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    load_dotenv(path.parent.parent / ".env", override=False)
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    p, d, s, q = raw.get("pipeline", {}), raw.get("database", {}), raw.get("schemas", {}), raw.get("quality", {})
    root = path.parent.parent
    env = lambda key, default: os.getenv(str(d.get(key, default)), "")
    url = URL.create("postgresql+psycopg", username=env("user_env", "DB_USER") or "garment_etl",
        password=env("password_env", "DB_PASSWORD"), host=env("host_env", "DB_HOST") or "localhost",
        port=int(env("port_env", "DB_PORT") or 5432), database=env("name_env", "DB_NAME") or "garment_dw",
        query={"sslmode": env("sslmode_env", "DB_SSLMODE") or "prefer"}).render_as_string(hide_password=False)
    config = Config(
        name=str(p.get("name", "garment_orders_pipeline")),
        batch_size=int(p.get("batch_size", 1000)),
        source_dir=(root / str(p.get("source_dir", "../synthetic_garment_source/data/generated/v1.0.0/orders"))).resolve(),
        source_file=str(p.get("source_file", "garment_orders.csv")),
        source_format=str(p.get("source_format", "auto")).lower(),
        rejected_dir=(root / str(p.get("rejected_dir", "../data/rejected"))).resolve(),
        log_dir=(root / str(p.get("log_dir", "../logs"))).resolve(),
        skip_completed_files=bool(p.get("skip_completed_files", True)),
        create_schemas=bool(p.get("create_schemas", True)),
        db_url=url,
        schemas={key: str(s.get(key, key)) for key in ("raw", "staging", "integration", "warehouse", "audit")},
        allowed_currencies=tuple(str(x).upper() for x in q.get("allowed_currencies", ["INR"])),
        allowed_statuses=tuple(str(x).upper() for x in q.get("allowed_statuses", [])),
        require_order_date_before_receive=bool(q.get("require_order_date_before_receive", True)),
        config_path=path,
    )
    if config.batch_size < 1:
        raise ValueError("pipeline.batch_size must be greater than zero")
    if config.source_format not in {"auto", "csv", "jsonl", "parquet"}:
        raise ValueError("source_format must be auto, csv, jsonl, or parquet")
    for name in config.schemas.values():
        if not name.replace("_", "").isalnum():
            raise ValueError(f"Invalid schema identifier: {name}")
    return config
