import os
from pathlib import Path

# --- Project Paths ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
os.makedirs(DATA_DIR, exist_ok=True)

DB_NAME = "market_data.db"
DB_PATH = f"sqlite:///{DATA_DIR / DB_NAME}"

# --- Trading Universe ---
# These must match the symbols you type into the HTML file
PAIRS = [
    {"symbol": "BTCUSDT", "role": "independent"},
    {"symbol": "ETHUSDT", "role": "dependent"},
]

# --- Analytics Defaults ---
REFRESH_RATE_MS = 1000      # Default Frontend refresh rate
DEFAULT_TIMEFRAME = "1s"
ROLLING_WINDOW = 60
Z_SCORE_ENTRY = 2.0
Z_SCORE_EXIT = 0.0

# --- UI Settings ---
PAGE_TITLE = "AlphaTrawler | Bridge Mode"
LAYOUT = "wide"