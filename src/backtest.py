import pandas as pd
import numpy as np


def run_backtest(spread_series, z_score_series, entry_threshold=2.0, exit_threshold=0.0):
    """
    Vectorized Mean Reversion Backtest with complete trade statistics.

    Returns:
        dict with:
            - equity_curve: pd.Series
            - total_return: float
            - sharpe_ratio: float
            - num_trades: int
            - win_rate: float
            - avg_win: float
            - avg_loss: float
            - positions: pd.Series
            - max_drawdown: float (min drawdown, negative)
    """
    df = pd.DataFrame({'spread': spread_series, 'z': z_score_series})
    df.dropna(inplace=True)

    if len(df) < 2:
        empty_series = pd.Series(dtype=float)
        return {
            "equity_curve": empty_series,
            "total_return": 0.0,
            "sharpe_ratio": 0.0,
            "num_trades": 0,
            "win_rate": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "positions": pd.Series(dtype=int),
            "max_drawdown": 0.0,
            "trades": []
        }

    positions = np.zeros(len(df))
    current_pos = 0
    z_vals = df['z'].values
    trades = []           # list of per-trade PnL
    entry_price = None
    entry_index = None

    for i in range(len(df)):
        z = z_vals[i]
        spread = df['spread'].iloc[i]

        if current_pos == 0:
            # Entry conditions
            if z > entry_threshold:
                current_pos = -1  # Short spread
                entry_price = spread
                entry_index = df.index[i]
            elif z < -entry_threshold:
                current_pos = 1   # Long spread
                entry_price = spread
                entry_index = df.index[i]

        elif current_pos == 1:
            # Long spread, exit when mean reverts
            if z >= exit_threshold:
                pnl = spread - entry_price
                trades.append({
                    "direction": "LONG",
                    "entry_time": entry_index,
                    "exit_time": df.index[i],
                    "entry_spread": entry_price,
                    "exit_spread": spread,
                    "pnl": pnl
                })
                current_pos = 0
                entry_price = None
                entry_index = None

        elif current_pos == -1:
            # Short spread, exit when mean reverts
            if z <= -exit_threshold:
                pnl = entry_price - spread
                trades.append({
                    "direction": "SHORT",
                    "entry_time": entry_index,
                    "exit_time": df.index[i],
                    "entry_spread": entry_price,
                    "exit_spread": spread,
                    "pnl": pnl
                })
                current_pos = 0
                entry_price = None
                entry_index = None

        positions[i] = current_pos

    df['position'] = positions
    df['spread_chg'] = df['spread'].diff()
    df['pnl'] = df['position'].shift(1) * df['spread_chg']
    df['pnl'].fillna(0.0, inplace=True)
    df['cumulative_pnl'] = df['pnl'].cumsum()

    # Equity metrics
    equity_curve = df['cumulative_pnl']
    total_return = float(equity_curve.iloc[-1]) if not equity_curve.empty else 0.0
    pnl_std = df['pnl'].std()
    sharpe = (df['pnl'].mean() / pnl_std) * np.sqrt(252 * 1440) if pnl_std and pnl_std != 0 else 0.0

    # Max drawdown on equity curve
    if equity_curve.empty:
        max_drawdown = 0.0
    else:
        equity_shifted = equity_curve + (abs(equity_curve.min()) + 1.0)
        roll_max = equity_shifted.cummax()
        drawdown = (equity_shifted - roll_max) / roll_max
        max_drawdown = float(drawdown.min())

    # Trade statistics
    num_trades = len(trades)
    if num_trades > 0:
        pnl_values = [t["pnl"] for t in trades]
        winning_trades = [p for p in pnl_values if p > 0]
        losing_trades = [p for p in pnl_values if p <= 0]

        win_rate = len(winning_trades) / num_trades if num_trades > 0 else 0.0
        avg_win = float(np.mean(winning_trades)) if winning_trades else 0.0
        avg_loss = float(np.mean(losing_trades)) if losing_trades else 0.0
    else:
        win_rate = 0.0
        avg_win = 0.0
        avg_loss = 0.0

    return {
        "equity_curve": equity_curve,
        "total_return": total_return,
        "sharpe_ratio": sharpe,
        "num_trades": num_trades,
        "win_rate": win_rate,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "positions": df['position'],
        "max_drawdown": max_drawdown,
        "trades": trades
    }
