from pathlib import Path
from garment_etl.readers import read_records, detect_format

def test_csv_reader(tmp_path: Path):
    p=tmp_path/"orders.csv"; p.write_text("order_id,quantity\nORD1,100\n",encoding="utf-8")
    assert list(read_records(p)) == [{"order_id":"ORD1","quantity":"100"}]
    assert detect_format(p)=="csv"

def test_jsonl_reader(tmp_path: Path):
    p=tmp_path/"orders.jsonl"; p.write_text('{"order_id":"ORD1"}\n',encoding="utf-8")
    assert list(read_records(p)) == [{"order_id":"ORD1"}]

def test_unknown_extension(tmp_path: Path):
    p=tmp_path/"x.txt"; p.write_text("x",encoding="utf-8")
    try: detect_format(p)
    except ValueError: pass
    else: raise AssertionError("Expected unsupported extension error")
