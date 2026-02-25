from sqlalchemy import Column, String, Float, Integer, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from datetime import datetime
from .db import Base

class Customer(Base):
    __tablename__ = "customers"
    cpf = Column(String(11), primary_key=True, index=True)
    name = Column(String(200), nullable=False)
    email = Column(String(200), nullable=True)
    accounts = relationship("Account", back_populates="owner", cascade="all, delete-orphan")

class Account(Base):
    __tablename__ = "accounts"
    id = Column(String(20), primary_key=True, index=True)
    owner_cpf = Column(String(11), ForeignKey("customers.cpf"), nullable=False)
    balance = Column(Float, default=0.0)
    owner = relationship("Customer", back_populates="accounts")
    transactions = relationship("Transaction", back_populates="account", cascade="all, delete-orphan")

class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String(20), ForeignKey("accounts.id"), nullable=False, index=True)
    date = Column(DateTime, default=datetime.utcnow)
    type = Column(String(20))
    amount = Column(Float)
    balance_after = Column(Float)
    description = Column(Text)
    account = relationship("Account", back_populates="transactions")
