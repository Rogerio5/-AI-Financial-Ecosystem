from sqlalchemy.orm import Session
from .models import Customer, Account, Transaction
from datetime import datetime

def create_customer(db: Session, name: str, cpf: str, email: str = ""):
    if db.get(Customer, cpf):
        return db.get(Customer, cpf)
    c = Customer(cpf=cpf, name=name, email=email)
    db.add(c)
    db.commit()
    db.refresh(c)
    return c

def create_account(db: Session, account_id: str, owner_cpf: str, initial_balance: float = 0.0):
    if db.get(Account, account_id):
        return db.get(Account, account_id)
    acc = Account(id=account_id, owner_cpf=owner_cpf, balance=initial_balance)
    db.add(acc)
    db.commit()
    db.refresh(acc)
    if initial_balance > 0:
        t = Transaction(account_id=account_id, date=datetime.utcnow(), type="deposit", amount=initial_balance, balance_after=initial_balance, description="Initial deposit")
        db.add(t)
        db.commit()
    return acc

def deposit(db: Session, account_id: str, amount: float, description: str = ""):
    acc = db.get(Account, account_id)
    if not acc:
        raise ValueError("Account not found")
    acc.balance += amount
    t = Transaction(account_id=account_id, date=datetime.utcnow(), type="deposit", amount=amount, balance_after=acc.balance, description=description)
    db.add(t)
    db.commit()
    return acc

def withdraw(db: Session, account_id: str, amount: float, description: str = ""):
    acc = db.get(Account, account_id)
    if not acc:
        raise ValueError("Account not found")
    if amount > acc.balance:
        raise ValueError("Insufficient funds")
    acc.balance -= amount
    t = Transaction(account_id=account_id, date=datetime.utcnow(), type="withdraw", amount=amount, balance_after=acc.balance, description=description)
    db.add(t)
    db.commit()
    return acc

def transfer(db: Session, from_id: str, to_id: str, amount: float, description: str = ""):
    withdraw(db, from_id, amount, f"transfer to {to_id}: {description}")
    deposit(db, to_id, amount, f"transfer from {from_id}: {description}")
