import os

import pytest
from pipeline.ingest_dft_traffic import main as ingest_dft_main
from pipeline.ingestion_os_roads import main as ingest_os_main

# Example test for DFT ingestion script
def test_ingest_dft_runs(monkeypatch):
    # Mock dependencies or file I/O as needed
    monkeypatch.setattr("builtins.print", lambda *a, **k: None)
    try:
        ingest_dft_main()
    except Exception as e:
        pytest.fail(f"ingest_dft_traffic main failed: {e}")

# Example test for OS Roads ingestion script
@pytest.mark.skipif(
    not os.environ.get("OS_API_KEY"),
    reason="OS_API_KEY not set — skipping OS Roads download test",
)
def test_ingest_os_roads_runs(monkeypatch):
    monkeypatch.setattr("builtins.print", lambda *a, **k: None)
    try:
        ingest_os_main()
    except Exception as e:
        pytest.fail(f"ingestion_os_roads main failed: {e}")
