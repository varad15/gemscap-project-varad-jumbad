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
    page_title="AlphaTrawler | Quantum Bridge",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="⚡"
)

# Initialize Core Services
init_db()
logger = setup_logger("Dashboard")

# Background Thread for Bridge Server (Port 8765) - only start once
if "ingestion_thread" not in st.session_state:
    try:
        st.session_state.ingestion_thread = threading.Thread(target=run_ingestor_sync, daemon=True)
        st.session_state.ingestion_thread.start()
        st.session_state.bridge_started = True
    except Exception as e:
        logger.warning(f"Bridge server thread already running or failed to start: {e}")
        st.session_state.bridge_started = False

if "ingestion_started" not in st.session_state:
    st.session_state.ingestion_started = True

# Session State
if "alerts_history" not in st.session_state:
    st.session_state.alerts_history = []
if "tick_counter" not in st.session_state:
    st.session_state.tick_counter = 0
if "last_signal" not in st.session_state:
    st.session_state.last_signal = None

# ==========================================
# 2. CYBERPUNK QUANT THEME
# ==========================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;700&family=Orbitron:wght@400;700;900&display=swap');

    /* --- CORE BACKGROUND --- */
    .stApp {
        background: 
            linear-gradient(180deg, #000000 0%, #0a0a0f 50%, #000000 100%),
            radial-gradient(ellipse at 20% 30%, rgba(0, 255, 157, 0.08) 0%, transparent 50%),
            radial-gradient(ellipse at 80% 70%, rgba(138, 43, 226, 0.08) 0%, transparent 50%);
        background-attachment: fixed;
        font-family: 'JetBrains Mono', monospace;
        color: #e0e0e0;
    }

    /* Animated grid background */
    .stApp::before {
        content: '';
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background-image: 
            linear-gradient(rgba(0, 255, 157, 0.03) 1px, transparent 1px),
            linear-gradient(90deg, rgba(0, 255, 157, 0.03) 1px, transparent 1px);
        background-size: 50px 50px;
        z-index: -1;
        animation: gridScroll 20s linear infinite;
    }

    @keyframes gridScroll {
        0% { transform: translateY(0); }
        100% { transform: translateY(50px); }
    }

    /* --- NEON GLOW SYSTEM --- */
    @keyframes neonPulse {
        0%, 100% { text-shadow: 0 0 10px rgba(0, 255, 157, 0.8), 0 0 20px rgba(0, 255, 157, 0.4); }
        50% { text-shadow: 0 0 20px rgba(0, 255, 157, 1), 0 0 40px rgba(0, 255, 157, 0.6); }
    }

    @keyframes borderGlow {
        0%, 100% { border-color: rgba(0, 255, 157, 0.4); box-shadow: 0 0 15px rgba(0, 255, 157, 0.3); }
        50% { border-color: rgba(0, 255, 157, 0.8); box-shadow: 0 0 30px rgba(0, 255, 157, 0.5); }
    }

    /* --- GLASS MORPHISM METRICS --- */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(0, 20, 20, 0.6) 0%, rgba(0, 10, 10, 0.4) 100%);
        backdrop-filter: blur(20px) saturate(180%);
        border: 2px solid rgba(0, 255, 157, 0.2);
        border-radius: 16px;
        padding: 20px;
        box-shadow: 
            0 8px 32px rgba(0, 0, 0, 0.4),
            inset 0 1px 0 rgba(255, 255, 255, 0.1);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        position: relative;
        overflow: hidden;
    }

    div[data-testid="stMetric"]::before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 100%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(0, 255, 157, 0.1), transparent);
        transition: left 0.5s;
    }

    div[data-testid="stMetric"]:hover {
        transform: translateY(-4px) scale(1.02);
        border-color: rgba(0, 255, 157, 0.6);
        box-shadow: 
            0 12px 48px rgba(0, 255, 157, 0.2),
            inset 0 1px 0 rgba(255, 255, 255, 0.2);
    }

    div[data-testid="stMetric"]:hover::before {
        left: 100%;
    }

    /* --- METRIC TYPOGRAPHY --- */
    div[data-testid="stMetricLabel"] {
        color: #00ff9d !important;
        font-size: 11px !important;
        font-weight: 700 !important;
        letter-spacing: 3px !important;
        text-transform: uppercase !important;
        font-family: 'Orbitron', sans-serif !important;
        opacity: 0.9;
    }

    div[data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-size: 36px !important;
        font-weight: 700 !important;
        font-family: 'Orbitron', sans-serif !important;
        text-shadow: 0 0 20px rgba(0, 255, 157, 0.5);
        letter-spacing: 1px;
    }

    div[data-testid="stMetricDelta"] {
        font-size: 13px !important;
        font-weight: 600 !important;
    }

    /* --- HOLOGRAPHIC TABS --- */
    .stTabs [data-baseweb="tab-list"] {
        gap: 12px;
        background: linear-gradient(90deg, rgba(0, 255, 157, 0.05) 0%, rgba(138, 43, 226, 0.05) 100%);
        border-radius: 12px;
        padding: 8px;
        border: 1px solid rgba(0, 255, 157, 0.1);
    }

    .stTabs [data-baseweb="tab"] {
        height: 56px;
        min-width: 160px;
        background: linear-gradient(135deg, rgba(0, 20, 20, 0.8) 0%, rgba(0, 10, 10, 0.6) 100%);
        border: 2px solid rgba(0, 255, 157, 0.2);
        border-radius: 10px;
        color: #888;
        font-family: 'Orbitron', sans-serif;
        font-size: 13px;
        font-weight: 700;
        letter-spacing: 2px;
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }

    .stTabs [data-baseweb="tab"]::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: linear-gradient(45deg, transparent, rgba(0, 255, 157, 0.1), transparent);
        transform: translateX(-100%);
        transition: transform 0.6s;
    }

    .stTabs [data-baseweb="tab"]:hover::before {
        transform: translateX(100%);
    }

    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #00ff9d 0%, #00d4ff 100%) !important;
        color: #000 !important;
        border: 2px solid #00ff9d !important;
        box-shadow: 
            0 0 30px rgba(0, 255, 157, 0.6),
            0 0 60px rgba(0, 255, 157, 0.3),
            inset 0 1px 0 rgba(255, 255, 255, 0.3);
        animation: borderGlow 2s ease-in-out infinite;
    }

    /* --- QUANTUM TICKER TAPE --- */
    .quantum-ticker {
        width: 100%;
        overflow: hidden;
        background: linear-gradient(90deg, 
            rgba(0, 0, 0, 0.9) 0%, 
            rgba(0, 20, 20, 0.95) 50%, 
            rgba(0, 0, 0, 0.9) 100%);
        border: 1px solid rgba(0, 255, 157, 0.3);
        border-radius: 8px;
        margin-bottom: 24px;
        box-shadow: 0 4px 24px rgba(0, 255, 157, 0.2);
        position: relative;
    }

    .quantum-ticker::before,
    .quantum-ticker::after {
        content: '';
        position: absolute;
        top: 0;
        width: 100px;
        height: 100%;
        z-index: 2;
    }

    .quantum-ticker::before {
        left: 0;
        background: linear-gradient(90deg, rgba(0, 0, 0, 0.9), transparent);
    }

    .quantum-ticker::after {
        right: 0;
        background: linear-gradient(270deg, rgba(0, 0, 0, 0.9), transparent);
    }

    .ticker-content {
        display: inline-block;
        white-space: nowrap;
        animation: tickerScroll 45s linear infinite;
        padding: 16px 0;
        font-family: 'Orbitron', sans-serif;
        font-size: 13px;
        font-weight: 700;
        letter-spacing: 2px;
    }

    @keyframes tickerScroll {
        0% { transform: translateX(100%); }
        100% { transform: translateX(-100%); }
    }

    .ticker-item {
        display: inline-block;
        margin: 0 40px;
        color: #00ff9d;
        text-shadow: 0 0 10px rgba(0, 255, 157, 0.5);
    }

    .ticker-separator {
        display: inline-block;
        margin: 0 20px;
        color: rgba(0, 255, 157, 0.3);
    }

    .ticker-value {
        color: #fff;
        font-weight: 900;
        text-shadow: 0 0 15px rgba(255, 255, 255, 0.5);
    }

    /* --- ALERT SYSTEM --- */
    .alert-container {
        background: linear-gradient(135deg, rgba(20, 0, 0, 0.8) 0%, rgba(10, 0, 0, 0.6) 100%);
        backdrop-filter: blur(15px);
        border: 2px solid rgba(255, 0, 85, 0.4);
        border-radius: 12px;
        padding: 16px;
        margin: 8px 0;
        position: relative;
        overflow: hidden;
        animation: alertPulse 2s ease-in-out infinite;
    }

    @keyframes alertPulse {
        0%, 100% { border-color: rgba(255, 0, 85, 0.4); box-shadow: 0 0 20px rgba(255, 0, 85, 0.2); }
        50% { border-color: rgba(255, 0, 85, 0.8); box-shadow: 0 0 40px rgba(255, 0, 85, 0.4); }
    }

    .log-container {
        max-height: 400px;
        overflow-y: auto;
        padding: 12px;
        background: rgba(0, 0, 0, 0.3);
        border-radius: 8px;
        border: 1px solid rgba(0, 255, 157, 0.1);
    }

    .log-container::-webkit-scrollbar {
        width: 8px;
    }

    .log-container::-webkit-scrollbar-track {
        background: rgba(0, 0, 0, 0.2);
        border-radius: 4px;
    }

    .log-container::-webkit-scrollbar-thumb {
        background: linear-gradient(180deg, #00ff9d, #8a2be2);
        border-radius: 4px;
    }

    .log-item {
        font-size: 12px;
        padding: 12px 16px;
        margin-bottom: 8px;
        border-radius: 8px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        transition: all 0.2s;
        border-left: 4px solid;
        animation: slideIn 0.3s ease-out;
    }

    @keyframes slideIn {
        from { transform: translateX(-20px); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }

    .log-buy {
        background: linear-gradient(90deg, rgba(0, 255, 136, 0.15) 0%, rgba(0, 255, 136, 0.05) 100%);
        border-left-color: #00ff88;
        color: #00ff88;
    }

    .log-sell {
        background: linear-gradient(90deg, rgba(255, 0, 85, 0.15) 0%, rgba(255, 0, 85, 0.05) 100%);
        border-left-color: #ff0055;
        color: #ff0055;
    }

    .log-item:hover {
        transform: translateX(4px);
        box-shadow: 0 4px 16px rgba(0, 255, 157, 0.2);
    }

    /* --- SIDEBAR STYLING --- */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(0, 10, 10, 0.95) 0%, rgba(0, 5, 5, 0.98) 100%);
        border-right: 1px solid rgba(0, 255, 157, 0.2);
    }

    section[data-testid="stSidebar"] > div {
        background: transparent;
    }

    .sidebar .block-container {
        padding-top: 2rem;
    }

    /* --- ENHANCED BUTTONS --- */
    .stButton > button {
        background: linear-gradient(135deg, rgba(0, 255, 157, 0.2) 0%, rgba(0, 212, 255, 0.2) 100%);
        border: 2px solid rgba(0, 255, 157, 0.4);
        border-radius: 8px;
        color: #00ff9d;
        font-family: 'Orbitron', sans-serif;
        font-weight: 700;
        letter-spacing: 1px;
        padding: 12px 24px;
        transition: all 0.3s ease;
    }

    .stButton > button:hover {
        background: linear-gradient(135deg, #00ff9d 0%, #00d4ff 100%);
        color: #000;
        border-color: #00ff9d;
        box-shadow: 0 0 30px rgba(0, 255, 157, 0.5);
        transform: translateY(-2px);
    }

    /* --- TITLE STYLING --- */
    h1 {
        font-family: 'Orbitron', sans-serif !important;
        font-weight: 900 !important;
        font-size: 48px !important;
        background: linear-gradient(135deg, #00ff9d 0%, #00d4ff 50%, #8a2be2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        animation: neonPulse 3s ease-in-out infinite;
        letter-spacing: 3px;
        margin-bottom: 0 !important;
    }

    h2, h3, h4 {
        font-family: 'Orbitron', sans-serif !important;
        color: #00ff9d !important;
        text-shadow: 0 0 10px rgba(0, 255, 157, 0.3);
    }

    /* --- STATUS INDICATOR --- */
    .status-online {
        display: inline-block;
        width: 12px;
        height: 12px;
        background: #00ff9d;
        border-radius: 50%;
        box-shadow: 0 0 20px rgba(0, 255, 157, 0.8);
        animation: statusPulse 2s ease-in-out infinite;
        margin-right: 8px;
    }

    @keyframes statusPulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.6; transform: scale(1.2); }
    }

    .status-error {
        display: inline-block;
        width: 12px;
        height: 12px;
        background: #ff0055;
        border-radius: 50%;
        box-shadow: 0 0 20px rgba(255, 0, 85, 0.8);
        animation: statusPulse 1s ease-in-out infinite;
        margin-right: 8px;
    }

    /* --- DATA TABLE --- */
    .stDataFrame {
        background: rgba(0, 0, 0, 0.4);
        border: 1px solid rgba(0, 255, 157, 0.2);
        border-radius: 8px;
    }

    /* --- DIVIDER --- */
    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(0, 255, 157, 0.5), transparent);
        margin: 24px 0;
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 3. ENHANCED ANALYTICS ENGINE
# ==========================================
class MetricsEngine:
    """Safe calculations for financial ratios."""

    @staticmethod
    def safe_divide(a, b, default=0.0):
        try:
            return a / b if b != 0 and not np.isnan(b) and not np.isinf(b) else default
        except Exception:
            return default

    @staticmethod
    def calculate_drawdown(equity_curve):
        if equity_curve.empty:
            return 0.0
        try:
            base = abs(equity_curve.min()) + 1000
            curve = equity_curve + base
            roll_max = curve.cummax()
            dd = (curve - roll_max) / roll_max
            return dd.min()
        except Exception:
            return 0.0

    @staticmethod
    def generate_report(backtest_res):
        defaults = {"max_dd": 0.0, "sortino": 0.0, "calmar": 0.0, "sharpe": 0.0, "total_return": 0.0}
        if not backtest_res or backtest_res.get('num_trades', 0) == 0:
            return defaults

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
        except Exception:
            return defaults

# ==========================================
# 4. CYBERPUNK VISUALIZATION ENGINE
# ==========================================
class QuantVisualizer:
    @staticmethod
    def _apply_theme(fig, height=400):
        """Apply cyberpunk theme to figure. ALWAYS returns a Figure object."""
        if fig is None:
            fig = go.Figure()

        try:
            fig.update_layout(
                template="plotly_dark",
                height=height,
                margin=dict(l=20, r=20, t=40, b=20),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,10,10,0.3)',
                font=dict(family="JetBrains Mono", size=11, color="#00ff9d"),
                xaxis=dict(
                    showgrid=True,
                    gridcolor='rgba(0, 255, 157, 0.1)',
                    gridwidth=1,
                    zeroline=False,
                    showline=True,
                    linecolor='rgba(0, 255, 157, 0.3)'
                ),
                yaxis=dict(
                    showgrid=True,
                    gridcolor='rgba(0, 255, 157, 0.1)',
                    gridwidth=1,
                    zeroline=True,
                    zerolinecolor='rgba(0, 255, 157, 0.3)',
                    showline=True,
                    linecolor='rgba(0, 255, 157, 0.3)'
                ),
                hovermode="x unified",
                hoverlabel=dict(
                    bgcolor="rgba(0, 20, 20, 0.9)",
                    font_size=12,
                    font_family="JetBrains Mono",
                    bordercolor="rgba(0, 255, 157, 0.5)"
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="left",
                    x=0,
                    bgcolor='rgba(0, 0, 0, 0.5)',
                    bordercolor='rgba(0, 255, 157, 0.3)',
                    borderwidth=1
                )
            )
        except Exception as e:
            logger.warning(f"_apply_theme layout error: {e}")

        # Explicitly return the themed figure
        return fig

    @staticmethod
    def plot_market_overlay(df, target, ref):
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Primary asset with glow effect
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df[target],
                name=target,
                line=dict(color='#00ff9d', width=3, shape='spline'),
                fill='tozeroy',
                fillcolor='rgba(0, 255, 157, 0.1)',
                hovertemplate='<b>$%{y:.2f}</b><extra></extra>'
            ),
            secondary_y=False
        )

        # Reference asset
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df[ref],
                name=ref,
                line=dict(color='#8a2be2', width=2, shape='spline', dash='dot'),
                hovertemplate='<b>%{y:.2f}</b><extra></extra>'
            ),
            secondary_y=True
        )

        # Drawdown zones
        cummax_target = df[target].cummax()
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=cummax_target.values,
                mode='lines',
                name='Peak',
                line=dict(color='rgba(255, 255, 255, 0.2)', width=1, dash='dot'),
                hovertemplate='Peak: <b>$%{y:.2f}</b><extra></extra>'
            ),
            secondary_y=False
        )

        fig.update_yaxes(title_text=f"<b>{target}</b>", secondary_y=False, title_font=dict(color='#00ff9d'))
        fig.update_yaxes(title_text=f"<b>{ref}</b>", secondary_y=True, title_font=dict(color='#8a2be2'))

        fig.update_layout(
            title=dict(
                text='<b>STRATEGY PERFORMANCE</b>',
                font=dict(family='Orbitron', size=16, color='#00ff9d')
            )
        )

        return QuantVisualizer._apply_theme(fig, height=450)

    @staticmethod
    def plot_signals(df, entry, exit):
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            row_heights=[0.65, 0.35],
            vertical_spacing=0.05,
            subplot_titles=('<b>Z-SCORE SIGNAL</b>', '<b>HEDGE RATIO (BETA)</b>')
        )

        # Z-Score with gradient fill
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df['z_score'],
                name="Z-Score",
                line=dict(color='#00d4ff', width=2, shape='spline'),
                fill='tozeroy',
                fillcolor='rgba(0, 212, 255, 0.1)',
                hovertemplate='<b>Z: %{y:.3f}</b><extra></extra>'
            ),
            row=1, col=1
        )

        # Entry/Exit bands with glow
        fig.add_hline(
            y=entry,
            line_dash="dash",
            line_color="#ff0055",
            line_width=2,
            annotation_text="SHORT ENTRY",
            annotation_position="right",
            annotation_font_color="#ff0055",
            row=1, col=1
        )
        fig.add_hline(
            y=-entry,
            line_dash="dash",
            line_color="#00ff88",
            line_width=2,
            annotation_text="LONG ENTRY",
            annotation_position="right",
            annotation_font_color="#00ff88",
            row=1, col=1
        )
        fig.add_hline(
            y=exit,
            line_dash="dot",
            line_color="rgba(255, 255, 255, 0.3)",
            line_width=1,
            row=1, col=1
        )

        # Beta with gradient
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df['beta'],
                name="Beta",
                line=dict(color='#f1fa8c', width=2, shape='spline'),
                fill='tozeroy',
                fillcolor='rgba(241, 250, 140, 0.08)',
                hovertemplate='<b>β: %{y:.4f}</b><extra></extra>'
            ),
            row=2, col=1
        )

        fig.update_annotations(font=dict(color='#00ff9d', size=12, family='Orbitron'))

        return QuantVisualizer._apply_theme(fig, height=550)

    @staticmethod
    def plot_heatmap(df, target, ref):
        window_sizes = [15, 30, 60, 120]
        corr_data = []
        for w in window_sizes:
            corr_data.append(df[target].rolling(w).corr(df[ref]).iloc[-50:].values)

        z_data = np.nan_to_num(np.array(corr_data))

        fig = go.Figure(
            data=go.Heatmap(
                z=z_data,
                x=df.index[-50:],
                y=[f"{w}p" for w in window_sizes],
                colorscale=[[0, '#1a0033'], [0.5, '#8a2be2'], [1, '#00ff9d']],
                hovertemplate='Window: %{y}<br>Time: %{x}<br>Corr: %{z:.3f}<extra></extra>',
                colorbar=dict(
                    title="ρ",
                    tickmode="linear",
                    tick0=0,
                    dtick=0.2,
                    thickness=15,
                    len=0.7
                )
            )
        )

        fig.update_layout(
            title=dict(
                text='<b>ROLLING CORRELATION MATRIX</b>',
                font=dict(family='Orbitron', size=14, color='#00ff9d')
            )
        )

        return QuantVisualizer._apply_theme(fig, height=280)

    @staticmethod
    def plot_scatter(df, target, ref):
        samp = df.tail(300)

        fig = px.scatter(
            samp,
            x=ref,
            y=target,
            trendline="ols",
            title=f'<b>REGRESSION: {target} ~ {ref}</b>',
            trendline_color_override='#ff0055'
        )

        fig.update_traces(
            marker=dict(
                size=6,
                color='#00ff9d',
                opacity=0.6,
                line=dict(color='#00d4ff', width=1)
            ),
            selector=dict(mode='markers')
        )

        return QuantVisualizer._apply_theme(fig, height=350)

    @staticmethod
    def plot_equity(equity_curve):
        df = equity_curve.rename("Equity")

        fig = go.Figure()

        # Main equity curve with gradient fill
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df.values,
                mode='lines',
                name='Equity',
                line=dict(color='#00ff9d', width=3, shape='spline'),
                fill='tozeroy',
                fillcolor='rgba(0, 255, 157, 0.15)',
                hovertemplate='<b>$%{y:.2f}</b><extra></extra>'
            )
        )

        # Add peak line
        cummax = df.cummax()
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=cummax.values,
                mode='lines',
                name='Peak',
                line=dict(color='rgba(255, 255, 255, 0.3)', width=1, dash='dot'),
                hovertemplate='Peak: <b>$%{y:.2f}</b><extra></extra>'
            )
        )

        fig.update_layout(
            title=dict(
                text='<b>EQUITY CURVE</b>',
                font=dict(family='Orbitron', size=16, color='#00ff9d')
            )
        )

        # Critical fix: ensure the figure is returned
        return QuantVisualizer._apply_theme(fig, height=400)

# ==========================================
# 5. MAIN CONTROLLER
# ==========================================
def main():
    # --- QUANTUM TICKER TAPE ---
    st.session_state.tick_counter += 1

    sess = SessionLocal()
    try:
        tick_count = sess.query(func.count(Tick.id)).scalar()
        bar_count = sess.query(func.count(Bar.id)).scalar()
    finally:
        sess.close()

    ticker_html = f"""
    <div class="quantum-ticker">
        <div class="ticker-content">
            <span class="ticker-item"><span class="status-online"></span>SYSTEM: <span class="ticker-value">OPERATIONAL</span></span>
            <span class="ticker-separator">◆</span>
            <span class="ticker-item">MODE: <span class="ticker-value">QUANTUM BRIDGE</span></span>
            <span class="ticker-separator">◆</span>
            <span class="ticker-item">PORT: <span class="ticker-value">8765</span></span>
            <span class="ticker-separator">◆</span>
            <span class="ticker-item">ALGO: <span class="ticker-value">KALMAN STAT-ARB</span></span>
            <span class="ticker-separator">◆</span>
            <span class="ticker-item">TICKS PROCESSED: <span class="ticker-value">{tick_count:,}</span></span>
            <span class="ticker-separator">◆</span>
            <span class="ticker-item">BARS FORMED: <span class="ticker-value">{bar_count:,}</span></span>
            <span class="ticker-separator">◆</span>
            <span class="ticker-item">LATENCY: <span class="ticker-value">&lt;500ms</span></span>
            <span class="ticker-separator">◆</span>
            <span class="ticker-item">TARGET: <span class="ticker-value">BTC/ETH</span></span>
        </div>
    </div>
    """
    st.markdown(ticker_html, unsafe_allow_html=True)

    # --- SIDEBAR CONTROL DECK ---
    with st.sidebar:
        st.markdown("## 🎛️ CONTROL DECK")
        st.markdown("---")

        # --- 1. DATA SOURCE ---
        st.markdown("### 1️⃣ DATA SOURCE")
        data_mode = st.radio(
            "Mode",
            ["🔌 HTML Bridge", "📂 Upload CSV"],
            help="Use Bridge for Live Trading, CSV for Backtesting"
        )

        uploaded_file = None
        if data_mode == "📂 Upload CSV":
            uploaded_file = st.file_uploader("Upload OHLC Data", type=['csv'])
        else:
            st.info("💡 Open `index.html` and click **START BRIDGE**")

        st.markdown("---")

        # --- 2. ASSETS ---
        st.markdown("### 2️⃣ ASSET PAIR")
        col1, col2 = st.columns(2)
        target = col1.selectbox("TARGET", [p['symbol'] for p in PAIRS], index=1)
        ref = col2.selectbox("REFERENCE", [p['symbol'] for p in PAIRS], index=0)

        st.markdown("---")

        # --- 3. PIPELINE ---
        st.markdown("### 3️⃣ PIPELINE")
        if data_mode == "🔌 HTML Bridge":
            freq = st.select_slider(
                "Timeframe",
                options=["1s", "5s", "1Min", "5Min"],
                value="1s"
            )
        else:
            freq = "N/A"

        # Validate positive window sizes
        win = st.number_input("Window Size", min_value=1, max_value=5000, value=60, step=10)

        st.markdown("---")

        # --- 4. STRATEGY ---
        st.markdown("### 4️⃣ STRATEGY")
        algo = st.radio("Model", ["Kalman Filter", "Rolling OLS"])

        col3, col4 = st.columns(2)
        z_in = col3.number_input("Entry Z", 1.0, 4.0, 2.0, 0.1)
        z_out = col4.number_input("Exit Z", -1.0, 1.0, 0.0, 0.1)

        # --- CUSTOM ALERT RULES ---
        st.markdown("### 🚨 ALERT RULES")
        enable_custom_alerts = st.checkbox("Enable custom z-score alerts", value=False)
        custom_z_upper = st.number_input("Custom Upper Z Alert", 0.5, 6.0, 2.0, 0.1)
        custom_z_lower = st.number_input("Custom Lower Z Alert", -6.0, -0.5, -2.0, 0.1)

        st.markdown("---")

        # --- 5. EXECUTION ---
        st.markdown("### 5️⃣ EXECUTION")
        if data_mode == "🔌 HTML Bridge":
            refresh_interval = st.slider(
                "Refresh Rate (s)",
                0.5, 5.0, 1.0, 0.5,
                help="Dashboard update frequency"
            )
        else:
            refresh_interval = 0

        st.markdown("---")

        # DB Stats
        st.markdown("### 📊 DATABASE")
        st.metric("Ticks", f"{tick_count:,}")
        st.metric("Bars", f"{bar_count:,}")

    # --- MAIN CONTENT AREA ---
    st.title(f"⚡ {target} / {ref}")
    st.markdown(
        "<p style='color: #888; font-size: 14px; margin-top: -10px;'>Real-time Statistical Arbitrage Engine</p>",
        unsafe_allow_html=True)

    # --- DATA LOADING & PROCESSING ---
    df = pd.DataFrame()
    connection_status = "online"

    if data_mode == "🔌 HTML Bridge":
        # LIVE PATH via BRIDGE
        proc = DataProcessor()
        proc.resample_ticks_to_bars(frequency=freq)
        df = proc.get_latest_bars([target, ref], limit=win * 5)

        if df.empty or len(df) < win:
            st.markdown("""
            <div class="alert-container">
                <h3>⏳ AWAITING DATA STREAM</h3>
                <p>Waiting for data from HTML Bridge...</p>
                <p style='font-size: 12px; color: #888;'>Please ensure <code>index.html</code> is open and streaming.</p>
            </div>
            """, unsafe_allow_html=True)
            time.sleep(1)
            st.rerun()
            return

        # Connection health check
        last_time = df.index[-1]
        time_diff = (datetime.now() - last_time).total_seconds()

        if time_diff > 2:
            connection_status = "error"
            st.markdown(f"""
            <div class="alert-container">
                <h3><span class="status-error"></span>BRIDGE DISCONNECTED</h3>
                <p>Last data received <strong>{int(time_diff)}</strong> seconds ago.</p>
                <p style='font-size: 12px;'>Action Required: Restart <code>index.html</code> and click START BRIDGE.</p>
            </div>
            """, unsafe_allow_html=True)

    elif data_mode == "📂 Upload CSV":
        # UPLOAD PATH
        if uploaded_file is None:
            st.info("👈 Upload a CSV file from the sidebar to begin analysis")
            return

        try:
            raw = pd.read_csv(uploaded_file)
            req_cols = {'timestamp', 'symbol', 'close'}
            if not req_cols.issubset(raw.columns):
                st.error(f"❌ CSV missing required columns: {req_cols}")
                return

            raw['timestamp'] = pd.to_datetime(raw['timestamp'])
            df = raw.pivot(index='timestamp', columns='symbol', values='close')

            if target not in df.columns or ref not in df.columns:
                st.error(f"❌ Symbols {target} or {ref} not found in CSV")
                return

            df = df[[target, ref]].dropna().sort_index()

        except Exception as e:
            st.error(f"❌ Error processing CSV: {e}")
            return

    # --- ANALYTICS ENGINE ---
    eng = AnalyticsEngine()
    try:
        # 1. Calculate Hedge Ratio
        if "Kalman" in algo:
            # Kalman now returns (beta, spread); use both
            beta, spread = eng.calculate_kalman_hedge_ratio(df[target], df[ref])
        else:
            beta, spread = eng.calculate_rolling_ols(df[target], df[ref], window=win)

        # 2. Calculate Z-Score
        z = eng.calculate_zscore(spread, window=win)

        # 3. Combine Data
        data = pd.DataFrame({
            target: df[target],
            ref: df[ref],
            'beta': beta,
            'spread': spread,
            'z_score': z
        }).dropna()

        if data.empty:
            st.warning("⚠️ Insufficient data after processing")
            return

        # 4. Signal Generation
        last = data.iloc[-1]
        sig_txt = "NEUTRAL"
        sig_color = "#888"

        if last['z_score'] > z_in:
            sig_txt = "SHORT SPREAD"
            sig_color = "#ff0055"
        elif last['z_score'] < -z_in:
            sig_txt = "LONG SPREAD"
            sig_color = "#00ff88"

        # 5. Alert System (configurable)
        if data_mode == "🔌 HTML Bridge":
            alert_triggered = False
            alert_type = None

            # Hard-coded strategy alerts (existing behavior)
            if sig_txt != "NEUTRAL":
                alert_triggered = True
                alert_type = 'sell' if 'SHORT' in sig_txt else 'buy'

            # Custom rule-based alerts
            if enable_custom_alerts:
                if last['z_score'] >= custom_z_upper:
                    alert_triggered = True
                    alert_type = 'sell'
                    sig_txt = f"CUSTOM SHORT ALERT (Z ≥ {custom_z_upper:.2f})"
                elif last['z_score'] <= custom_z_lower:
                    alert_triggered = True
                    alert_type = 'buy'
                    sig_txt = f"CUSTOM LONG ALERT (Z ≤ {custom_z_lower:.2f})"

            if alert_triggered:
                msg = f"{datetime.now().strftime('%H:%M:%S')} | {sig_txt} | Z: {last['z_score']:.2f}"
                if msg != st.session_state.last_signal:
                    st.session_state.alerts_history.append({
                        'msg': msg,
                        'type': alert_type,
                        'time': datetime.now()
                    })
                    st.session_state.last_signal = msg

        # 6. Backtest
        bt = run_backtest(data['spread'], data['z_score'], z_in, z_out)
        metrics = MetricsEngine.generate_report(bt)

    except Exception as e:
        st.error(f"⚠️ Analytics Engine Error: {e}")
        logger.error(f"Analytics error: {e}")
        return

    # --- KPI DASHBOARD ---
    st.markdown("<br>", unsafe_allow_html=True)

    kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

    with kpi1:
        st.metric(
            "SIGNAL",
            sig_txt,
            f"{last['z_score']:.2f}σ"
        )

    with kpi2:
        st.metric(
            "SPREAD",
            f"{last['spread']:.4f}",
            f"{(last['spread'] - data['spread'].mean()):.4f}"
        )

    with kpi3:
        st.metric(
            "HEDGE RATIO",
            f"{last['beta']:.3f}",
            f"{(last['beta'] - data['beta'].mean()):.3f}"
        )

    with kpi4:
        sharpe_delta = "Excellent" if metrics['sharpe'] > 2 else ("Good" if metrics['sharpe'] > 1 else "Poor")
        st.metric(
            "SHARPE",
            f"{metrics['sharpe']:.2f}",
            sharpe_delta
        )

    with kpi5:
        st.metric(
            "PNL",
            f"{metrics['total_return']:.4f}",
            f"DD: {metrics['max_dd']:.1%}"
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # --- TABBED INTERFACE ---
    tabs = st.tabs(["📈 OVERVIEW", "🧮 QUANT LAB", "🧪 BACKTEST", "💾 DATA"])

    # === TAB 1: OVERVIEW ===
    with tabs[0]:
        col_left, col_right = st.columns([3, 1])

        with col_left:
            st.plotly_chart(
                QuantVisualizer.plot_signals(data, z_in, z_out),
                use_container_width=True,
                config={'displayModeBar': False}
            )
            st.plotly_chart(
                QuantVisualizer.plot_market_overlay(data, target, ref),
                use_container_width=True,
                config={'displayModeBar': False}
            )

        with col_right:
            st.markdown("### 📜 SIGNAL LOG")

            with st.container(height=350):
                if data_mode == "📂 Upload CSV":
                    st.caption("Signal logging disabled in CSV mode")
                else:
                    if not st.session_state.alerts_history:
                        st.caption("No signals generated yet...")
                    else:
                        for alert in reversed(st.session_state.alerts_history[-25:]):
                            st.markdown(
                                f"<div class='log-item log-{alert['type']}'>"
                                f"<span>{alert['msg']}</span>"
                                f"</div>",
                                unsafe_allow_html=True
                            )

            st.markdown("---")
            st.markdown("### 🔥 CORRELATION")
            st.plotly_chart(
                QuantVisualizer.plot_heatmap(data, target, ref),
                use_container_width=True,
                config={'displayModeBar': False}
            )

    # === TAB 2: QUANT LAB ===
    with tabs[1]:
        lab_col1, lab_col2 = st.columns(2)

        with lab_col1:
            st.markdown("#### 📊 REGRESSION ANALYSIS")
            st.plotly_chart(
                QuantVisualizer.plot_scatter(data, target, ref),
                use_container_width=True,
                config={'displayModeBar': False}
            )

        with lab_col2:
            st.markdown("#### 🧬 STATIONARITY TEST")
            try:
                adf_stat, p_value, is_stat = eng.perform_adf_test(data['spread'])

                stat_col1, stat_col2 = st.columns(2)
                stat_col1.metric(
                    "ADF P-Value",
                    f"{p_value:.4f}",
                    "Stationary ✅" if is_stat else "Non-Stationary ⚠️"
                )
                stat_col2.metric(
                    "ADF Statistic",
                    f"{adf_stat:.2f}"
                )

                if is_stat:
                    st.success("✅ Spread is mean-reverting (p < 0.05)")
                else:
                    st.warning("⚠️ Spread may not be stationary")

            except Exception as e:
                st.error(f"Insufficient data for ADF test: {e}")

            st.markdown("---")
            st.markdown("#### 📊 Z-SCORE DISTRIBUTION")

            fig_hist = px.histogram(
                data,
                x="z_score",
                nbins=50,
                title="<b>Signal Distribution</b>"
            )
            fig_hist.update_traces(marker_color='#00ff9d', marker_line_color='#00d4ff', marker_line_width=1)
            fig_hist = QuantVisualizer._apply_theme(fig_hist, height=250)
            st.plotly_chart(fig_hist, use_container_width=True, config={'displayModeBar': False})

    # === TAB 3: BACKTEST ===
    with tabs[2]:
        perf_col1, perf_col2, perf_col3 = st.columns(3)

        with perf_col1:
            st.metric("SORTINO RATIO", f"{metrics['sortino']:.2f}")
        with perf_col2:
            st.metric("CALMAR RATIO", f"{metrics['calmar']:.2f}")
        with perf_col3:
            st.metric("MAX DRAWDOWN", f"{metrics['max_dd']:.2%}")

        st.markdown("---")

        # Simple equity plot + trade stats
        if bt and isinstance(bt, dict) and 'equity_curve' in bt:
            equity_data = bt['equity_curve']

            if equity_data is not None and not (hasattr(equity_data, 'empty') and equity_data.empty):
                equity_fig = QuantVisualizer.plot_equity(equity_data)

                if equity_fig is not None and isinstance(equity_fig, go.Figure):
                    st.plotly_chart(equity_fig, use_container_width=True, config={'displayModeBar': False})
                else:
                    st.error(f"❌ Invalid figure returned: type={type(equity_fig)}")

                # Trade Statistics (requires bt to include win_rate, avg_win, avg_loss)
                trade_col1, trade_col2, trade_col3, trade_col4 = st.columns(4)
                trade_col1.metric("Total Trades", bt.get('num_trades', 0))
                trade_col2.metric("Win Rate", f"{bt.get('win_rate', 0) * 100:.1f}%")
                trade_col3.metric("Avg Win", f"{bt.get('avg_win', 0):.4f}")
                trade_col4.metric("Avg Loss", f"{bt.get('avg_loss', 0):.4f}")
            else:
                st.info("📊 No equity data yet - waiting for trades to be generated")
        else:
            st.info("📊 Run backtest to view equity curve")

    # === TAB 4: DATA ===
    with tabs[3]:
        st.markdown("### 📋 PROCESSED DATA TABLE")
        st.dataframe(
            data.sort_index(ascending=False),
            use_container_width=True,
            height=500
        )

        # Download button for processed data
        csv = data.to_csv().encode('utf-8')
        st.download_button(
            label="📥 Download CSV (Processed)",
            data=csv,
            file_name=f"alphatrawler_{target}_{ref}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

        # Optional: export analytics outputs if available from analytics engine / backtest
        if bt and isinstance(bt, dict):
            analytics_export = {
                "equity_curve": bt.get("equity_curve"),
                "trade_log": bt.get("trades"),
                "sharpe_ratio": bt.get("sharpe_ratio"),
                "total_return": bt.get("total_return"),
            }
            try:
                # Basic flat export: equity curve and summary stats
                eq_df = pd.DataFrame({
                    "timestamp": analytics_export["equity_curve"].index,
                    "equity": analytics_export["equity_curve"].values
                }) if analytics_export["equity_curve"] is not None else pd.DataFrame()

                summary_df = pd.DataFrame([{
                    "sharpe_ratio": analytics_export["sharpe_ratio"],
                    "total_return": analytics_export["total_return"],
                    "sortino": metrics["sortino"],
                    "calmar": metrics["calmar"],
                    "max_drawdown": metrics["max_dd"]
                }])

                with pd.ExcelWriter("analytics_outputs.xlsx", engine="xlsxwriter") as writer:
                    if not eq_df.empty:
                        eq_df.to_excel(writer, sheet_name="EquityCurve", index=False)
                    summary_df.to_excel(writer, sheet_name="Summary", index=False)

                with open("analytics_outputs.xlsx", "rb") as f:
                    st.download_button(
                        label="📥 Download Analytics Outputs",
                        data=f,
                        file_name=f"analytics_outputs_{target}_{ref}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            except Exception as e:
                logger.warning(f"Analytics export failed: {e}")

    # --- AUTO-REFRESH FOR LIVE MODE ---
    if data_mode == "🔌 HTML Bridge" and connection_status == "online":
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main()
