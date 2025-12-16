import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller


class AnalyticsEngine:

    @staticmethod
    def calculate_rolling_ols(target: pd.Series, reference: pd.Series, window=60):
        """
        Performs rolling OLS to find the hedge ratio (beta).
        Spread = Y - beta * X
        Returns: (beta_series, spread_series)
        """
        # Align data and drop NaNs to ensure indices match
        df = pd.concat([target, reference], axis=1).dropna()

        # Critical Fix: Return empty series instead of None to prevent unpacking errors
        if len(df) < window:
            empty = pd.Series(dtype=float)
            return empty, empty

        Y = df.iloc[:, 0]  # Dependent (Target)
        X = df.iloc[:, 1]  # Independent (Reference)

        hedge_ratios = []
        spreads = []

        # Rolling OLS Calculation
        for i in range(window, len(df)):
            y_window = Y.iloc[i - window:i]
            x_window = X.iloc[i - window:i]
            x_window = sm.add_constant(x_window)

            try:
                model = sm.OLS(y_window, x_window).fit()
                beta = model.params.iloc[1]

                current_spread = Y.iloc[i] - (beta * X.iloc[i])

                hedge_ratios.append(beta)
                spreads.append(current_spread)
            except Exception:
                hedge_ratios.append(np.nan)
                spreads.append(np.nan)

        pad_len = window
        pad = [np.nan] * pad_len

        beta_series = pd.Series(pad + hedge_ratios, index=df.index)
        spread_series = pd.Series(pad + spreads, index=df.index)

        return beta_series, spread_series

    @staticmethod
    def calculate_kalman_hedge_ratio(target, reference, delta=1e-5, vt=1e-3):
        """
        Dynamic Hedge Ratio using a Kalman Filter.
        State: [beta, alpha] (Slope, Intercept)
        Measurement: Target Price (Y)
        Observation Matrix: [Reference Price (X), 1]

        Returns:
            (beta_series, spread_series)
        """
        df = pd.concat([target, reference], axis=1).dropna()
        if df.empty:
            empty = pd.Series(dtype=float)
            return empty, empty

        y = df.iloc[:, 0].values
        x = df.iloc[:, 1].values
        n = len(y)

        # State Vector: [beta, alpha]
        state_mean = np.zeros((2, 1))

        # Covariance Matrix (start with high uncertainty)
        P = np.eye(2) * 10.0

        # Process Noise Covariance (Q)
        Q = np.eye(2) * delta

        # Measurement Noise Covariance (R)
        R = vt

        betas = np.zeros(n)
        spreads = np.zeros(n)

        for t in range(n):
            # Prediction Step
            state_pred = state_mean
            P_pred = P + Q

            # Observation Matrix H = [x_t, 1]
            H = np.array([[x[t], 1.0]])

            y_pred = H @ state_pred
            error = y[t] - y_pred

            S = H @ P_pred @ H.T + R

            try:
                K = P_pred @ H.T @ np.linalg.inv(S)
            except np.linalg.LinAlgError:
                K = np.zeros((2, 1))

            state_mean = state_pred + K * error
            P = (np.eye(2) - K @ H) @ P_pred

            betas[t] = state_mean[0, 0]
            spreads[t] = error.item()

        beta_series = pd.Series(betas, index=df.index)
        spread_series = pd.Series(spreads, index=df.index)

        # Important: return a tuple (beta, spread) exactly as app.py expects
        return beta_series, spread_series

    @staticmethod
    def calculate_zscore(series, window=60):
        """
        Robust rolling Z-Score calculation.
        """
        if series is None or len(series) == 0:
            return pd.Series(dtype=float)

        roll_mean = series.rolling(window=window).mean()
        roll_std = series.rolling(window=window).std()

        zscore = (series - roll_mean) / roll_std.replace(0, np.nan)
        return zscore

    @staticmethod
    def perform_adf_test(series):
        """
        Augmented Dickey-Fuller test for stationarity.
        Returns: (Statistic, p-value, is_stationary)
        """
        clean_series = series.dropna()
        if len(clean_series) < 30:
            return 0.0, 1.0, False

        try:
            result = adfuller(clean_series, maxlag=1)
            stat = result[0]
            p_value = result[1]
            is_stationary = p_value < 0.05
            return stat, p_value, is_stationary
        except Exception:
            return 0.0, 1.0, False
