import typer
from bankpy.db import SessionLocal, engine
from bankpy.models import Base
from bankpy.crud import create_customer, create_account, deposit, withdraw, transfer
import scripts.etl_db as etl

app = typer.Typer()

@app.command()
def init_db():
    """Cria schema no banco (se ainda não existir)"""
    Base.metadata.create_all(bind=engine)
    typer.echo("Schema criado.")

@app.command()
def seed():
    """Aplica seed no banco (scripts/seed_db.py)"""
    import scripts.seed_db as s
    s.run_seed()

@app.command()
def listar_clientes():
    db = SessionLocal()
    rows = db.execute("SELECT cpf, name, email FROM customers").all()
    for r in rows:
        typer.echo(f"{r.name} | CPF: {r.cpf} | Email: {r.email}")
    db.close()

@app.command()
def criar_cliente(nome: str, cpf: str, email: str = ""):
    db = SessionLocal()
    try:
        create_customer(db, nome, cpf, email)
        typer.echo(f"Cliente {nome} criado.")
    except Exception as e:
        typer.echo(str(e))
    finally:
        db.close()

@app.command()
def abrir_conta(account_id: str, cpf: str, saldo_inicial: float = 0.0):
    db = SessionLocal()
    try:
        create_account(db, account_id, cpf, saldo_inicial)
        typer.echo(f"Conta {account_id} aberta para {cpf}.")
    except Exception as e:
        typer.echo(str(e))
    finally:
        db.close()

@app.command()
def depositar(account_id: str, amount: float):
    db = SessionLocal()
    try:
        deposit(db, account_id, amount, "deposit via CLI")
        typer.echo(f"Depósito de {amount:.2f} realizado.")
    except Exception as e:
        typer.echo(str(e))
    finally:
        db.close()

@app.command()
def sacar(account_id: str, amount: float):
    db = SessionLocal()
    try:
        withdraw(db, account_id, amount, "saque via CLI")
        typer.echo(f"Saque de {amount:.2f} realizado.")
    except Exception as e:
        typer.echo(str(e))
    finally:
        db.close()

@app.command()
def transferir(from_id: str, to_id: str, amount: float):
    db = SessionLocal()
    try:
        transfer(db, from_id, to_id, amount, "transfer via CLI")
        typer.echo(f"Transferência de {amount:.2f} realizada.")
    except Exception as e:
        typer.echo(str(e))
    finally:
        db.close()

@app.command()
def gerar_etl():
    etl.run_etl()

if __name__ == "__main__":
    app()
