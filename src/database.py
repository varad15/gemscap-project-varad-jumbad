# src/database.py
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Index
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import src.config as config

Base = declarative_base()


class Tick(Base):
    """
    Raw trade data from Binance.
    High-write frequency.
    """
    __tablename__ = 'ticks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    quantity = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)

    # Composite index for faster range queries on symbols
    __table_args__ = (Index('idx_symbol_timestamp', 'symbol', 'timestamp'),)

    def __repr__(self):
        return f"<Tick(symbol={self.symbol}, price={self.price}, time={self.timestamp})>"


class Bar(Base):
    """
    Resampled OHLCV bars (1-second base resolution).
    Used to speed up analytics so we don't always resample millions of ticks.
    """
    __tablename__ = 'bars'

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    timestamp = Column(DateTime, index=True)  # Bar start time

    __table_args__ = (Index('idx_bar_sym_time', 'symbol', 'timestamp'),)


# --- Engine & Session Factory ---
engine = create_engine(
    config.DB_PATH,
    connect_args={"check_same_thread": False},  # Needed for SQLite with multi-threading
    echo=False  # Set to True for SQL debugging
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Creates tables if they don't exist."""
    Base.metadata.create_all(bind=engine)


def get_db():
    """Dependency for DB sessions."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()