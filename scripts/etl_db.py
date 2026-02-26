import pandas as pd
from pathlib import Path
from sqlalchemy import text
from bankpy.db import engine

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
out_csv = DATA_DIR / "transactions_flat.csv"

def run_etl():
    query = """
    SELECT t.id, t.account_id, a.owner_cpf, t.date, t.type, t.amount, t.balance_after, t.description
    FROM transactions t
    JOIN accounts a ON t.account_id = a.id
    ORDER BY t.date;
    """
    df = pd.read_sql(text(query), con=engine)
    if df.empty:
        print("Nenhuma transação encontrada.")
        return
    df.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"ETL concluído. CSV gerado em: {out_csv}")

if __name__ == "__main__":
    run_etl()
