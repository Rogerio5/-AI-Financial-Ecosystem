from bankpy.db import engine, SessionLocal, Base
from bankpy.crud import create_customer, create_account, deposit
from sqlalchemy.exc import SQLAlchemyError

def create_schema():
    Base.metadata.create_all(bind=engine)

def run_seed():
    create_schema()
    db = SessionLocal()
    try:
        create_customer(db, "Rogério Silva", "12345678901", "rogerio.silva@example.com")
        create_customer(db, "Mariana Costa", "98765432100", "mariana.costa@example.com")
        create_account(db, "0001-CC", "12345678901", initial_balance=1250.50)
        create_account(db, "0002-CC", "98765432100", initial_balance=500.00)
        deposit(db, "0001-CC", 250.50, "Depósito salário")
        print("Seed aplicada com sucesso no banco PostgreSQL.")
    except SQLAlchemyError as e:
        print("Erro ao aplicar seed:", e)
    finally:
        db.close()

if __name__ == "__main__":
    run_seed()
