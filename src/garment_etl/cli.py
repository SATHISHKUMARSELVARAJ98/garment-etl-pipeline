import argparse, json, logging, sys
from pathlib import Path
from sqlalchemy import text
from garment_etl.config import load_config
from garment_etl.database import initialize_database, make_engine
from garment_etl.pipeline import run_pipeline
from garment_etl.readers import read_records
from garment_etl.validation import normalize_order

def parser():
    p=argparse.ArgumentParser(description="Garment ERP ETL Pipeline")
    p.add_argument("--config",default="config/dev.yaml")
    sub=p.add_subparsers(dest="command",required=True)
    sub.add_parser("init-db")
    run = sub.add_parser("run")
    run.add_argument("--source")
    run.add_argument(
        "--force-baseline",
        action="store_true",
        help="Reprocess files to initialize record-level hashes",
    )
    val=sub.add_parser("validate-file"); val.add_argument("--source",required=True); val.add_argument("--limit",type=int,default=0)
    sub.add_parser("status")
    return p

def main(argv=None):
    args=parser().parse_args(argv)
    try:
        config=load_config(args.config)
        config.log_dir.mkdir(parents=True,exist_ok=True)
        logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s",
            handlers=[logging.StreamHandler(),logging.FileHandler(config.log_dir/"garment_etl.log",encoding="utf-8")])
        logger=logging.getLogger("garment_etl")
        if args.command=="validate-file":
            count=bad=0
            for row in read_records(Path(args.source).resolve(),config.source_format,config.batch_size):
                count+=1; _, errors=normalize_order(row,config.allowed_currencies,config.allowed_statuses,config.require_order_date_before_receive)
                if errors: bad+=1; print(f"Record {count}: {'; '.join(errors)}")
                if args.limit and count>=args.limit: break
            print(json.dumps({"records_checked":count,"records_with_errors":bad},indent=2)); return 1 if bad else 0
        engine=make_engine(config.db_url)
        if args.command=="init-db":
            initialize_database(engine,config.schemas); print("Database schemas and tables initialized."); return 0
        if args.command=="run":
            if config.create_schemas: initialize_database(engine,config.schemas)
            result = run_pipeline(
                config,
                engine,
                logger,
                Path(args.source).resolve() if args.source else None,
                force_baseline=args.force_baseline,
            )
            print(json.dumps(result,indent=2,default=str)); return 0 if result["status"]=="SUCCEEDED" else 1
        if args.command=="status":
            a=config.schemas["audit"]
            with engine.connect() as conn:
                rows=conn.execute(text(f'''SELECT run_id,pipeline_name,started_at,finished_at,status,files_seen,files_skipped,
                    files_succeeded,files_failed,rows_read,rows_accepted,rows_rejected FROM "{a}".pipeline_run
                    ORDER BY run_id DESC LIMIT 20''')).mappings().all()
            print(json.dumps([dict(x) for x in rows],indent=2,default=str)); return 0
    except Exception as exc:
        print(f"ERROR: {exc}",file=sys.stderr); return 2
    return 0

if __name__=="__main__": raise SystemExit(main())
