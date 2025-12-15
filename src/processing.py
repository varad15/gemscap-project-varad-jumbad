import pandas as pd
import numpy as np
from sqlalchemy import select, func
from src.database import SessionLocal, Tick, Bar
from src.utils import setup_logger

logger = setup_logger("Resampler")


class DataProcessor:
    def __init__(self):
        self.session = SessionLocal()

    def resample_ticks_to_bars(self, frequency='1s'):
        """
        Reads raw ticks from SQLite, resamples them to OHLCV bars, 
        and stores them in the 'bars' table.
        """
        # 1. Find the timestamp of the last bar to avoid reprocessing
        try:
            last_bar_time = self.session.query(func.max(Bar.timestamp)).scalar()
        except Exception:
            self.session.rollback()
            last_bar_time = None

        query = select(Tick)
        if last_bar_time:
            query = query.where(Tick.timestamp > last_bar_time)

        # Load ticks into Pandas
        try:
            df_ticks = pd.read_sql(query, self.session.bind)
        except Exception as e:
            logger.error(f"Error reading ticks: {e}")
            return

        if df_ticks.empty:
            return

        # 2. Vectorized Resampling Grouped by Symbol
        df_ticks['timestamp'] = pd.to_datetime(df_ticks['timestamp'])
        df_ticks.set_index('timestamp', inplace=True)

        new_bars = []

        for symbol, group in df_ticks.groupby('symbol'):
            # Resample logic
            ohlc = group['price'].resample(frequency, label='left', closed='left').ohlc()
            vol = group['quantity'].resample(frequency, label='left', closed='left').sum()

            # Combine and clean
            bars_df = pd.concat([ohlc, vol], axis=1).dropna()
            bars_df.columns = ['open', 'high', 'low', 'close', 'volume']
            bars_df['symbol'] = symbol

            # Reset index to make timestamp a column
            bars_df.reset_index(inplace=True)

            # Convert to list of dicts for bulk insert
            new_bars.extend(bars_df.to_dict('records'))

        # 3. Bulk Write to DB
        if new_bars:
            try:
                self.session.bulk_insert_mappings(Bar, new_bars)
                self.session.commit()
            except Exception as e:
                logger.error(f"Error saving bars: {e}")
                self.session.rollback()

    def get_latest_bars(self, symbols: list, limit=1000) -> pd.DataFrame:
        """
        Fetches the latest N bars for analytics, pivoted by symbol.
        """
        # Query latest data
        query = select(Bar).where(Bar.symbol.in_(symbols)).order_by(Bar.timestamp.desc()).limit(limit * len(symbols))
        df = pd.read_sql(query, self.session.bind)

        if df.empty:
            return pd.DataFrame()

        # --- FIX: Deduplicate before pivoting ---
        # This handles cases where restarts caused duplicate overlapping bars
        df = df.drop_duplicates(subset=['timestamp', 'symbol'], keep='last')

        # Pivot: Index=Time, Columns=Symbol
        try:
            df = df.pivot(index='timestamp', columns='symbol', values='close')
            df.sort_index(inplace=True)
            return df.ffill().dropna()
        except ValueError as e:
            logger.error(f"Pivot error: {e}")
            return pd.DataFrame()