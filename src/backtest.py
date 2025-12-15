# src/backtest.py
import pandas as pd
import numpy as np


def run_backtest(spread_series, z_score_series, entry_threshold=2.0, exit_threshold=0.0):
    """
    Vectorized Mean Reversion Backtest.

    Logic:
    - Short Spread when Z > threshold (Expect spread to go down)
    - Long Spread when Z < -threshold (Expect spread to go up)
    - Exit when Z crosses exit_threshold (mean reversion)
    """
    df = pd.DataFrame({'spread': spread_series, 'z': z_score_series})
    df.dropna(inplace=True)

    # Positions: 1 (Long), -1 (Short), 0 (Flat)
    df['position'] = 0

    # --- Entry Logic ---
    # Long Entry
    df.loc[df['z'] < -entry_threshold, 'position'] = 1
    # Short Entry
    df.loc[df['z'] > entry_threshold, 'position'] = -1

    # --- Exit Logic (trickier in vectorization) ---
    # We essentially want to forward-fill the position until the exit condition is met.
    # Logic:
    # If we are Long (1), we stay Long until Z >= exit_threshold.
    # If we are Short (-1), we stay Short until Z <= -exit_threshold (assuming symmetric 0 exit).

    # Create signals only on change
    df['signal'] = 0
    df.loc[df['z'] < -entry_threshold, 'signal'] = 1
    df.loc[df['z'] > entry_threshold, 'signal'] = -1

    # Mark exits (crossing 0)
    # Using 0.1 buffer to ensure we actually crossed mean
    df.loc[abs(df['z']) < 0.1, 'signal'] = 0

    # This is a simplified "Continuous" backtest.
    # For strict state-machine behavior (entry -> hold -> exit),
    # loop is often safer, but let's try a hybrid approach.

    positions = np.zeros(len(df))
    current_pos = 0
    z_vals = df['z'].values

    # Optimized loop (Numba would be better, but standard python is fast enough for <10k rows)
    for i in range(len(df)):
        z = z_vals[i]

        if current_pos == 0:
            if z > entry_threshold:
                current_pos = -1  # Short spread
            elif z < -entry_threshold:
                current_pos = 1  # Long spread
        elif current_pos == 1:
            if z >= exit_threshold:
                current_pos = 0  # Exit Long
        elif current_pos == -1:
            if z <= -exit_threshold:
                current_pos = 0  # Exit Short

        positions[i] = current_pos

    df['position'] = positions

    # Calculate Returns
    # Spread PnL = Position(t-1) * (Spread(t) - Spread(t-1))
    df['spread_chg'] = df['spread'].diff()
    df['pnl'] = df['position'].shift(1) * df['spread_chg']

    # Cumulative PnL
    df['cumulative_pnl'] = df['pnl'].cumsum()

    # Metrics
    total_return = df['cumulative_pnl'].iloc[-1]
    sharpe = (df['pnl'].mean() / df['pnl'].std()) * np.sqrt(252 * 1440) if df['pnl'].std() != 0 else 0
    trades = df['position'].diff().abs().sum() / 2

    return {
        "equity_curve": df['cumulative_pnl'],
        "total_return": total_return,
        "sharpe_ratio": sharpe,
        "num_trades": int(trades),
        "positions": df['position']
    }