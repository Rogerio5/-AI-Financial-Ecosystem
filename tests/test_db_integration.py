import subprocess
import sys
from pathlib import Path

def test_seed_and_etl():
    subprocess.run([sys.executable, "-m", "bankpy.scripts.seed_db"], check=True)
    subprocess.run([sys.executable, "scripts/etl_db.py"], check=True)
    csv_file = Path("data") / "transactions_flat.csv"
    assert csv_file.exists()
