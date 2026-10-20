import json
from types import SimpleNamespace
from unittest.mock import MagicMock
from garment_etl.cli import parser,main
import garment_etl.cli as cli


def test_parser_accepts_init_db():
    args = parser().parse_args(["init-db"])

    assert args.command == "init-db"


def test_parser_accepts_run():
    args = parser().parse_args(["run"])

    assert args.command == "run"
    assert args.source is None
    assert args.force_baseline is False


def test_parser_accepts_run_with_source_and_force_baseline():
    args = parser().parse_args(
        [
            "--config",
            "config/test.yaml",
            "run",
            "--source",
            "tests/fixtures/test_orders.csv",
            "--force-baseline",
        ]
    )

    assert args.config == "config/test.yaml"
    assert args.command == "run"
    assert args.source == "tests/fixtures/test_orders.csv"
    assert args.force_baseline is True


def test_parser_accepts_validate_file():
    args = parser().parse_args(
        ["validate-file", "--source", "tests/fixtures/test_orders.csv", "--limit", "10"]
    )

    assert args.command == "validate-file"
    assert args.source == "tests/fixtures/test_orders.csv"
    assert args.limit == 10


def test_parser_accepts_status():
    args = parser().parse_args(["status"])
    assert args.command == "status"
def test_validate_file_accepts_valid_records(capsys):
    result = main([
        "validate-file",
        "--source", "tests/fixtures/valid_orders.csv",
    ])

    captured = capsys.readouterr()

    assert result == 0
    assert '"records_checked": 1' in captured.out
    assert '"records_with_errors": 0' in captured.out


def test_validate_file_reports_invalid_records(capsys):
    result = main([
        "validate-file",
        "--source", "tests/fixtures/invalid_orders.csv",
    ])

    captured = capsys.readouterr()

    assert result == 1
    assert "quantity must be greater than zero" in captured.out
    assert '"records_with_errors": 1' in captured.out

def test_status_displays_pipeline_history(monkeypatch, capsys, tmp_path):
    fake_config = SimpleNamespace(
        log_dir=tmp_path,
        schemas={"audit": "audit"},
        db_url="postgresql://unused",
    )

    fake_row = {
        "run_id": 7,
        "pipeline_name": "garment_orders_test_pipeline",
        "started_at": "2026-10-20 09:00:00",
        "finished_at": "2026-10-20 09:01:00",
        "status": "SUCCEEDED",
        "files_seen": 1,
        "files_skipped": 0,
        "files_succeeded": 1,
        "files_failed": 0,
        "rows_read": 100,
        "rows_accepted": 98,
        "rows_rejected": 2,
    }

    fake_connection = MagicMock()
    fake_connection.execute.return_value.mappings.return_value.all.return_value = [
        fake_row
    ]

    fake_engine = MagicMock()
    fake_engine.connect.return_value.__enter__.return_value = fake_connection

    monkeypatch.setattr(cli, "load_config", lambda path: fake_config)
    monkeypatch.setattr(cli, "make_engine", lambda url: fake_engine)

    result = cli.main(["status"])

    captured = capsys.readouterr()
    output = json.loads(captured.out)

    assert result == 0
    assert len(output) == 1
    assert output[0]["run_id"] == 7
    assert output[0]["status"] == "SUCCEEDED"
    assert output[0]["rows_accepted"] == 98

def test_cli_returns_error_when_config_loading_fails(monkeypatch, capsys):
    def fail_config(path):
        raise RuntimeError("Configuration unavailable")

    monkeypatch.setattr(cli, "load_config", fail_config)

    result = cli.main(["status"])

    captured = capsys.readouterr()

    assert result == 2
    assert "ERROR: Configuration unavailable" in captured.err


def test_init_db_initializes_database(monkeypatch, capsys, tmp_path):
    fake_config = SimpleNamespace(
        log_dir=tmp_path,
        schemas={
            "raw": "raw",
            "staging": "staging",
            "integration": "integration",
            "warehouse": "warehouse",
            "audit": "audit",
        },
        db_url="postgresql://unused",
    )

    fake_engine = MagicMock()
    initialize_mock = MagicMock()

    monkeypatch.setattr(cli, "load_config", lambda path: fake_config)
    monkeypatch.setattr(cli, "make_engine", lambda url: fake_engine)
    monkeypatch.setattr(cli, "initialize_database", initialize_mock)

    result = cli.main(["init-db"])

    captured = capsys.readouterr()

    assert result == 0
    assert "Database schemas and tables initialized." in captured.out
    initialize_mock.assert_called_once_with(fake_engine, fake_config.schemas)


def test_run_command_returns_success_when_pipeline_succeeds(
    monkeypatch, capsys, tmp_path
):
    fake_config = SimpleNamespace(
        log_dir=tmp_path,
        db_url="postgresql://unused",
        create_schemas=False,
    )
    fake_engine = MagicMock()
    fake_result = {
        "status": "SUCCEEDED",
        "rows_read": 10,
        "rows_accepted": 10,
        "rows_rejected": 0,
    }
    pipeline_mock = MagicMock(return_value=fake_result)

    monkeypatch.setattr(cli, "load_config", lambda path: fake_config)
    monkeypatch.setattr(cli, "make_engine", lambda url: fake_engine)
    monkeypatch.setattr(cli, "run_pipeline", pipeline_mock)

    result = cli.main(["run"])

    captured = capsys.readouterr()
    output = json.loads(captured.out)

    assert result == 0
    assert output["status"] == "SUCCEEDED"
    pipeline_mock.assert_called_once()


def test_run_command_returns_failure_when_pipeline_fails(
    monkeypatch, capsys, tmp_path
):
    fake_config = SimpleNamespace(
        log_dir=tmp_path,
        db_url="postgresql://unused",
        create_schemas=False,
    )
    fake_engine = MagicMock()
    fake_result = {
        "status": "FAILED",
        "rows_read": 10,
        "rows_accepted": 8,
        "rows_rejected": 2,
    }
    pipeline_mock = MagicMock(return_value=fake_result)

    monkeypatch.setattr(cli, "load_config", lambda path: fake_config)
    monkeypatch.setattr(cli, "make_engine", lambda url: fake_engine)
    monkeypatch.setattr(cli, "run_pipeline", pipeline_mock)

    result = cli.main(["run"])

    captured = capsys.readouterr()
    output = json.loads(captured.out)

    assert result == 1
    assert output["status"] == "FAILED"
    pipeline_mock.assert_called_once()


def test_run_initializes_schemas_when_configured(monkeypatch, capsys, tmp_path):
    fake_config = SimpleNamespace(
        log_dir=tmp_path,
        db_url="postgresql://unused",
        create_schemas=True,
        schemas={
            "raw": "raw",
            "staging": "staging",
            "integration": "integration",
            "warehouse": "warehouse",
            "audit": "audit",
        },
    )

    fake_engine = MagicMock()
    initialize_mock = MagicMock()
    pipeline_mock = MagicMock(
        return_value={
            "status": "SUCCEEDED",
            "rows_read": 0,
            "rows_accepted": 0,
            "rows_rejected": 0,
        }
    )

    monkeypatch.setattr(cli, "load_config", lambda path: fake_config)
    monkeypatch.setattr(cli, "make_engine", lambda url: fake_engine)
    monkeypatch.setattr(cli, "initialize_database", initialize_mock)
    monkeypatch.setattr(cli, "run_pipeline", pipeline_mock)

    result = cli.main(["run"])

    capsys.readouterr()

    assert result == 0
    initialize_mock.assert_called_once_with(fake_engine, fake_config.schemas)
    pipeline_mock.assert_called_once()


def test_run_skips_schema_initialization_when_not_configured(
    monkeypatch, capsys, tmp_path
):
    fake_config = SimpleNamespace(
        log_dir=tmp_path,
        db_url="postgresql://unused",
        create_schemas=False,
        schemas={
            "raw": "raw",
            "staging": "staging",
            "integration": "integration",
            "warehouse": "warehouse",
            "audit": "audit",
        },
    )

    fake_engine = MagicMock()
    initialize_mock = MagicMock()
    pipeline_mock = MagicMock(
        return_value={
            "status": "SUCCEEDED",
            "rows_read": 0,
            "rows_accepted": 0,
            "rows_rejected": 0,
        }
    )

    monkeypatch.setattr(cli, "load_config", lambda path: fake_config)
    monkeypatch.setattr(cli, "make_engine", lambda url: fake_engine)
    monkeypatch.setattr(cli, "initialize_database", initialize_mock)
    monkeypatch.setattr(cli, "run_pipeline", pipeline_mock)

    result = cli.main(["run"])

    capsys.readouterr()

    assert result == 0
    initialize_mock.assert_not_called()
    pipeline_mock.assert_called_once()
