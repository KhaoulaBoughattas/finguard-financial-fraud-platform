
import pandas as pd

from src.generator.generate import run


def test_run_generates_expected_parquet_files_and_summary(tmp_path):
    output_dir = tmp_path / "raw"
    summary = run(
        num_customers=30,
        num_accounts_max_per_customer=2,
        num_merchants=15,
        num_transactions=300,
        fraud_rate=0.05,
        seed=42,
        output_dir=str(output_dir),
        persist_db=False,
    )

    assert summary["customers"] == 30
    assert summary["merchants"] == 15
    assert summary["transactions"] >= 300
    assert 0 <= summary["actual_fraud_rate"] <= 1
    assert "postgres" not in summary

    for name in ("customers", "accounts", "merchants", "transactions"):
        f = output_dir / f"{name}.parquet"
        assert f.exists()

    tx_df = pd.read_parquet(output_dir / "transactions.parquet")
    assert len(tx_df) == summary["transactions"]
    assert tx_df["is_fraud"].sum() > 0


def test_run_with_persist_db_writes_to_postgres(tmp_path):
    output_dir = tmp_path / "raw"
    summary = run(
        num_customers=10,
        num_accounts_max_per_customer=1,
        num_merchants=5,
        num_transactions=50,
        fraud_rate=0.1,
        seed=1,
        output_dir=str(output_dir),
        persist_db=True,
    )
    assert "postgres" in summary
    assert summary["postgres"]["customers_loaded"] == 10
    assert summary["postgres"]["merchants_loaded"] == 5
