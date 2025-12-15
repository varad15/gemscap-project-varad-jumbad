import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import threading
import time
from datetime import datetime, timedelta
from sqlalchemy import func

# --- Local Imports ---
from src.config import PAIRS, REFRESH_RATE_MS
from src.ingestion import run_ingestor_sync
from src.processing import DataProcessor
from src.analytics import AnalyticsEngine
from src.backtest import run_backtest
from src.database import init_db, SessionLocal, Tick, Bar
from src.utils import setup_logger

# ==========================================
# 1. SYSTEM KERNEL & BOOTSTRAP
# ==========================================
st.set_page_config(
    page_title="AlphaTrawler | Bridge",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="⚡"
)

# Initialize Core Services
init_db()
logger = setup_logger("Dashboard")

# Background Thread for Bridge Server (Port 8765)
if "ingestion_thread" not in st.session_state:
    st.session_state.ingestion_thread = threading.Thread(target=run_ingestor_sync, daemon=True)
    st.session_state.ingestion_thread.start()

# Session State
if "alerts_history" not in st.session_state: st.session_state.alerts_history = []

# ==========================================
# 2. "NEO-WALLSTREET" CSS THEME
# ==========================================
st.markdown("""
<style>
    /* --- CORE THEME --- */
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Oswald:wght@500&display=swap');

    .stApp {
        background-color: #050505;
        background-image: 
            radial-gradient(circle at 50% 50%, rgba(0, 50, 50, 0.1) 0%, transparent 50%),
            linear-gradient(rgba(255, 255, 255, 0.03) 1px, transparent 1px),
            linear-gradient(90deg, rgba(255, 255, 255, 0.03) 1px, transparent 1px);
        background-size: 100% 100%, 40px 40px, 40px 40px;
        font-family: 'JetBrains Mono', monospace;
    }

    /* --- GLASSMORPHISM PANELS --- */
    div[data-testid="stMetric"], .glass-card {
        background: rgba(14, 17, 23, 0.7);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 8px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.5);
        transition: transform 0.2s;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        border-color: rgba(0, 240, 255, 0.4);
        box-shadow: 0 8px 30px rgba(0, 240, 255, 0.1);
    }

    /* --- BIGGER, BOLDER TABS --- */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        border-bottom: 2px solid #222;
        padding-bottom: 5px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        min-width: 140px; /* Wider buttons */
        background-color: #111;
        border: 1px solid #333;
        color: #888;
        border-radius: 6px;
        font-family: 'Oswald', sans-serif; /* Strong font */
        font-size: 16px;
        letter-spacing: 1px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #00F0FF !important;
        color: #000 !important;
        border: 1px solid #00F0FF !important;
        box-shadow: 0 0 15px rgba(0, 240, 255, 0.6);
        font-weight: bold;
    }

    /* --- METRICS TYPOGRAPHY --- */
    div[data-testid="stMetricLabel"] {
        color: #888; font-size: 12px !important; letter-spacing: 2px; text-transform: uppercase; font-weight: 700;
    }
    div[data-testid="stMetricValue"] {
        color: #fff; font-size: 32px !important; font-weight: 700; text-shadow: 0 0 15px rgba(255,255,255,0.1);
    }

    /* --- TICKER TAPE --- */
    .ticker-container {
        width: 100%; overflow: hidden; background: #000; border-bottom: 1px solid #333; margin-bottom: 20px;
    }
    .ticker-text {
        display: inline-block; white-space: nowrap; animation: ticker 40s linear infinite; color: #00F0FF; font-size: 12px; padding: 8px 0;
    }
    @keyframes ticker { 0% { transform: translateX(100%); } 100% { transform: translateX(-100%); } }

    /* --- ALERTS & LOGS --- */
    .log-item {
        font-size: 11px; padding: 8px 12px; border-bottom: 1px solid #222; margin-bottom: 4px; border-radius: 4px;
        display: flex; justify-content: space-between; align-items: center;
    }
    .log-buy { background: rgba(0, 255, 136, 0.1); border-left: 3px solid #00FF88; color: #00FF88; }
    .log-sell { background: rgba(255, 0, 85, 0.1); border-left: 3px solid #FF0055; color: #FF0055; }
</style>
""", unsafe_allow_html=True)


# ==========================================
# 3. ROBUST ANALYTICS ENGINE
# ==========================================
class MetricsEngine:
    """Safe calculations for financial ratios."""

    @staticmethod
    def safe_divide(a, b, default=0.0):
        try:
            return a / b if b != 0 and not np.isnan(b) and not np.isinf(b) else default
        except:
            return default

    @staticmethod
    def calculate_drawdown(equity_curve):
        if equity_curve.empty: return 0.0
        try:
            base = abs(equity_curve.min()) + 1000
            curve = equity_curve + base
            roll_max = curve.cummax()
            dd = (curve - roll_max) / roll_max
            return dd.min()
        except:
            return 0.0

    @staticmethod
    def generate_report(backtest_res):
        defaults = {"max_dd": 0.0, "sortino": 0.0, "calmar": 0.0, "sharpe": 0.0, "total_return": 0.0}
        if not backtest_res or backtest_res.get('num_trades', 0) == 0: return defaults

        try:
            equity = backtest_res.get('equity_curve', pd.Series(dtype=float))
            max_dd = MetricsEngine.calculate_drawdown(equity)
            returns = equity.diff().fillna(0)
            downside = returns[returns < 0].std()

            sortino = MetricsEngine.safe_divide(returns.mean() * np.sqrt(252 * 1440), downside)
            total_ret = backtest_res.get('total_return', 0.0)
            calmar = MetricsEngine.safe_divide(total_ret, abs(max_dd * 1000))

            return {
                "max_dd": max_dd,
                "sortino": sortino,
                "calmar": calmar,
                "sharpe": backtest_res.get('sharpe_ratio', 0.0),
                "total_return": total_ret
            }
        except:
            return defaults


# ==========================================
# 4. QUANT VISUALIZATION (THEMED)
# ==========================================
class QuantVisualizer:
    @staticmethod
    def _apply_theme(fig, height=400):
        fig.update_layout(
            template="plotly_dark", height=height,
            margin=dict(l=0, r=0, t=30, b=0),
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            font=dict(family="JetBrains Mono", size=10, color="#888"),
            xaxis=dict(showgrid=False, zeroline=False),
            yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.05)', zeroline=False),
            hovermode="x unified",
            legend=dict(orientation="h", y=1.02, x=0, bgcolor='rgba(0,0,0,0)')
        )
        return fig

    @staticmethod
    def plot_market_overlay(df, target, ref):
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Scatter(x=df.index, y=df[target], name=target, line=dict(color='#00F0FF', width=2)),
                      secondary_y=False)
        fig.add_trace(
            go.Scatter(x=df.index, y=df[ref], name=ref, line=dict(color='rgba(255,255,255,0.4)', width=1, dash='dot')),
            secondary_y=True)
        return QuantVisualizer._apply_theme(fig)

    @staticmethod
    def plot_signals(df, entry, exit):
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.03)
        # Z-Score
        fig.add_trace(go.Scatter(
            x=df.index, y=df['z_score'], name="Z-Score", line=dict(color='#BD93F9', width=1.5),
            fill='tozeroy', fillcolor='rgba(189, 147, 249, 0.05)'
        ), row=1, col=1)
        # Bands
        fig.add_hline(y=entry, line_dash="dash", line_color="#FF5555", row=1, col=1)
        fig.add_hline(y=-entry, line_dash="dash", line_color="#50FA7B", row=1, col=1)
        # Beta
        fig.add_trace(go.Scatter(x=df.index, y=df['beta'], name="Beta", line=dict(color='#F1FA8C', width=1)), row=2,
                      col=1)
        return QuantVisualizer._apply_theme(fig, height=500)

    @staticmethod
    def plot_heatmap(df, target, ref):
        window_sizes = [15, 30, 60, 120]
        corr_data = []
        for w in window_sizes:
            corr_data.append(df[target].rolling(w).corr(df[ref]).iloc[-50:].values)
        z_data = np.nan_to_num(np.array(corr_data))
        fig = go.Figure(
            data=go.Heatmap(z=z_data, x=df.index[-50:], y=[f"W_{w}" for w in window_sizes], colorscale='Viridis'))
        fig.update_layout(title="Rolling Correlation Heatmap")
        return QuantVisualizer._apply_theme(fig, height=250)

    @staticmethod
    def plot_scatter(df, target, ref):
        samp = df.tail(300)
        fig = px.scatter(samp, x=ref, y=target, trendline="ols", title=f"Regression: {target} ~ {ref}")
        fig.update_traces(marker=dict(size=4, color='#00F0FF', opacity=0.7))
        return QuantVisualizer._apply_theme(fig, height=300)

    @staticmethod
    def plot_equity(equity_curve):
        df = equity_curve.rename("Equity")
        fig = px.area(df, title="Strategy Performance")
        fig.update_traces(line_color='#00F0FF', fillcolor='rgba(0, 240, 255, 0.1)')
        return QuantVisualizer._apply_theme(fig, height=350)


# ==========================================
# 5. MAIN CONTROLLER
# ==========================================
def main():
    # --- TICKER TAPE ---
    st.markdown("""
    <div class="ticker-container">
        <div class="ticker-text">
            SYSTEM STATUS: <span style="color:#00FF00">ONLINE</span> &nbsp;&bull;&nbsp; 
            MODE: HTML BRIDGE LISTENER (PORT 8765) &nbsp;&bull;&nbsp; 
            ALGO: KALMAN STAT-ARB &nbsp;&bull;&nbsp; 
            TARGET: BTC/ETH PAIR TRADING
        </div>
    </div>
    """, unsafe_allow_html=True)

    # --- SIDEBAR CONTROL DECK ---
    with st.sidebar:
        st.header("🎛️ CONTROL DECK")

        # --- 1. DATA SOURCE (MANDATORY FEATURE) ---
        st.subheader("1. DATA SOURCE")
        data_mode = st.radio("Mode", ["🔌 HTML Bridge", "📂 Upload CSV"], help="Use Bridge for Live, CSV for Backtest.")

        uploaded_file = None
        if data_mode == "📂 Upload CSV":
            uploaded_file = st.file_uploader("Upload OHLC Data", type=['csv'])
        else:
            st.info("ℹ️ Ensure `index.html` is open and 'START BRIDGE' is clicked.")

        # --- 2. ASSETS ---
        st.subheader("2. ASSETS")
        c1, c2 = st.columns(2)
        target = c1.selectbox("Y (Target)", [p['symbol'] for p in PAIRS], index=1)
        ref = c2.selectbox("X (Ref)", [p['symbol'] for p in PAIRS], index=0)

        # --- 3. PIPELINE ---
        st.subheader("3. PIPELINE")
        if data_mode == "🔌 HTML Bridge":
            freq = st.select_slider("Sampling", ["1s", "5s", "1Min", "5Min"], value="1s")
        else:
            freq = "N/A"

        win = st.number_input("Lookback Window", 20, 5000, 60)

        # --- 4. STRATEGY ---
        st.subheader("4. STRATEGY")
        algo = st.radio("Estimator", ["Kalman Filter", "Rolling OLS"])
        z_in = st.slider("Entry Z", 1.0, 4.0, 2.0, 0.1)
        z_out = st.slider("Exit Z", -1.0, 1.0, 0.0, 0.1)

        # --- 5. EXECUTION ---
        st.subheader("5. EXECUTION")
        refresh_interval = 0
        if data_mode == "🔌 HTML Bridge":
            refresh_interval = st.slider("Update Rate (s)", 0.5, 5.0, 1.0, 0.5)

            # DB Stats
            sess = SessionLocal()
            try:
                tc = sess.query(func.count(Tick.id)).scalar()
                bc = sess.query(func.count(Bar.id)).scalar()
                st.caption(f"Ticks Received: {tc} | Bars: {bc}")
            finally:
                sess.close()

    # --- MAIN CONTENT AREA ---
    st.title(f"{target} / {ref}")

    # --- DATA LOADING & PROCESSING ---
    df = pd.DataFrame()

    if data_mode == "🔌 HTML Bridge":
        # LIVE PATH via BRIDGE
        proc = DataProcessor()
        proc.resample_ticks_to_bars(frequency=freq)
        df = proc.get_latest_bars([target, ref], limit=win * 5)

        if df.empty or len(df) < win:
            st.warning("⏳ Waiting for data from HTML Bridge... (Please open index.html)")
            time.sleep(1)
            st.rerun()
            return

        # --- NEW: CONNECTION LOST ALERT (ADDED FEATURE) ---
        # Calculate time since last data point
        last_time = df.index[-1]
        time_diff = (datetime.now() - last_time).total_seconds()

        if time_diff > 2:  # If no data for >10 seconds
            st.error(
                f"🚨 BRIDGE DISCONNECTED: Last data received {int(time_diff)} seconds ago. Please restart 'index.html'.")
        # --------------------------------------------------

    elif data_mode == "📂 Upload CSV":
        # UPLOAD PATH
        if uploaded_file is None:
            st.info("👈 Waiting for CSV upload...")
            return

        try:
            raw = pd.read_csv(uploaded_file)
            req_cols = {'timestamp', 'symbol', 'close'}
            if not req_cols.issubset(raw.columns):
                st.error(f"CSV missing columns. Required: {req_cols}")
                return

            raw['timestamp'] = pd.to_datetime(raw['timestamp'])
            df = raw.pivot(index='timestamp', columns='symbol', values='close')

            if target not in df.columns or ref not in df.columns:
                st.error(f"Symbols {target} or {ref} not found in CSV.")
                return

            df = df[[target, ref]].dropna().sort_index()

        except Exception as e:
            st.error(f"Error reading CSV: {e}")
            return

    # --- ANALYTICS ENGINE ---
    eng = AnalyticsEngine()
    try:
        # 1. Calc Hedge Ratio
        if "Kalman" in algo:
            beta, spread = eng.calculate_kalman_hedge_ratio(df[target], df[ref])
        else:
            beta, spread = eng.calculate_rolling_ols(df[target], df[ref], window=win)

        # 2. Calc Z-Score
        z = eng.calculate_zscore(spread, window=win)

        # 3. Combine
        data = pd.DataFrame({
            target: df[target], ref: df[ref],
            'beta': beta, 'spread': spread, 'z_score': z
        }).dropna()

        if data.empty:
            st.warning("Insufficient data after processing.")
            return

        # 4. Signal Logic
        last = data.iloc[-1]
        sig_txt = "NEUTRAL"
        if last['z_score'] > z_in:
            sig_txt = "SHORT SPREAD"
        elif last['z_score'] < -z_in:
            sig_txt = "LONG SPREAD"

        # 5. Alerts (Bridge Mode Only)
        if data_mode == "🔌 HTML Bridge" and sig_txt != "NEUTRAL":
            msg = f"{datetime.now().strftime('%H:%M:%S')} | {sig_txt} | Z: {last['z_score']:.2f}"
            if not st.session_state.alerts_history or st.session_state.alerts_history[-1]['msg'] != msg:
                st.session_state.alerts_history.append({'msg': msg, 'type': 'sell' if 'SHORT' in sig_txt else 'buy'})

        # 6. Backtest
        bt = run_backtest(data['spread'], data['z_score'], z_in, z_out)
        metrics = MetricsEngine.generate_report(bt)

    except Exception as e:
        st.error(f"ENGINE FAILURE: {e}")
        return

    # --- DASHBOARD RENDERING ---

    # 1. KPI ROW (Bloomberg Style)
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("SIGNAL", sig_txt, f"{last['z_score']:.2f}", delta_color="off")
    k2.metric("SPREAD", f"{last['spread']:.4f}")
    k3.metric("BETA", f"{last['beta']:.3f}")
    k4.metric("SHARPE", f"{metrics['sharpe']:.2f}")
    k5.metric("PNL", f"{metrics['total_return']:.4f}", delta=f"DD: {metrics['max_dd']:.1%}")

    # 2. FEATURE TABS
    tabs = st.tabs(["📈 OVERVIEW", "🧮 QUANT LAB", "🧪 BACKTEST", "💾 DATA"])

    # Tab 1: Overview
    with tabs[0]:
        c1, c2 = st.columns([3, 1])
        with c1:
            st.plotly_chart(QuantVisualizer.plot_signals(data, z_in, z_out), use_container_width=True,
                            config={'scrollZoom': True})
            st.plotly_chart(QuantVisualizer.plot_market_overlay(data, target, ref), use_container_width=True,
                            config={'scrollZoom': True})
        with c2:
            st.markdown("### 📜 SIGNAL LOG")
            with st.container(height=300):
                if data_mode == "📂 Upload CSV":
                    st.caption("Logs disabled in CSV mode.")
                else:
                    for a in reversed(st.session_state.alerts_history[-20:]):
                        st.markdown(f"<div class='log-item log-{a['type']}'><span>{a['msg']}</span></div>",
                                    unsafe_allow_html=True)

            st.markdown("### 🔥 CORRELATION")
            st.plotly_chart(QuantVisualizer.plot_heatmap(data, target, ref), use_container_width=True,
                            config={'scrollZoom': True})

    # Tab 2: Quant Lab
    with tabs[1]:
        q1, q2 = st.columns(2)
        with q1:
            st.markdown("#### REGRESSION ANALYSIS")
            st.plotly_chart(QuantVisualizer.plot_scatter(data, target, ref), use_container_width=True,
                            config={'scrollZoom': True})
        with q2:
            st.markdown("#### STATIONARITY (ADF)")
            # --- ADF TEST IMPLEMENTATION ---
            try:
                adf_stat, p_value, is_stat = eng.perform_adf_test(data['spread'])
                st.metric("ADF P-Value", f"{p_value:.4f}",
                          delta="Stationary ✅" if is_stat else "Non-Stationary ⚠️",
                          delta_color="normal" if is_stat else "inverse")
                st.caption(f"ADF Stat: {adf_stat:.2f}")
            except:
                st.warning("Insufficient data for ADF.")

            st.divider()
            st.markdown("#### SIGNAL DISTRIBUTION")
            fig_hist = px.histogram(data, x="z_score", nbins=50)
            fig_hist = QuantVisualizer._apply_theme(fig_hist, height=200)
            fig_hist.update_traces(marker_color='#BD93F9')
            st.plotly_chart(fig_hist, use_container_width=True, config={'scrollZoom': True})

    # Tab 3: Performance
    with tabs[2]:
        p1, p2 = st.columns(2)
        p1.metric("SORTINO RATIO", f"{metrics['sortino']:.2f}")
        p2.metric("CALMAR RATIO", f"{metrics['calmar']:.2f}")

        if 'equity_curve' in bt:
            st.plotly_chart(QuantVisualizer.plot_equity(bt['equity_curve']), use_container_width=True,
                            config={'scrollZoom': True})

    # Tab 4: Data
    with tabs[3]:
        st.dataframe(data.sort_index(ascending=False), use_container_width=True)

    # Live Refresh Loop (Controlled by Slider)
    if data_mode == "🔌 HTML Bridge":
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()