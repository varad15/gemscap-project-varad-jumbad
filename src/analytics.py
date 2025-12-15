# src/analytics.py
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from scipy.stats import norm


class AnalyticsEngine:

    @staticmethod
    def calculate_rolling_ols(target: pd.Series, reference: pd.Series, window=60):
        """
        Performs rolling OLS to find the hedge ratio (beta).
        Spread = Y - beta * X
        """
        # Align data
        df = pd.concat([target, reference], axis=1).dropna()
        if len(df) < window:
            return None, None, None

        Y = df.iloc[:, 0]  # Dependent (ETH)
        X = df.iloc[:, 1]  # Independent (BTC)

        hedge_ratios = []
        spreads = []

        # Optimized rolling window
        # Note: For massive datasets, we would use stride_tricks or Numba.
        # For <10k points, simple loop is fine and readable.
        for i in range(window, len(df)):
            y_window = Y.iloc[i - window:i]
            x_window = X.iloc[i - window:i]
            x_window = sm.add_constant(x_window)  # Add intercept

            model = sm.OLS(y_window, x_window).fit()
            beta = model.params.iloc[1]

            # Calculate spread for the CURRENT point using the regression from the window
            # This avoids look-ahead bias (we trade based on past relationship)
            current_spread = Y.iloc[i] - beta * X.iloc[i]

            hedge_ratios.append(beta)
            spreads.append(current_spread)

        # Pad initial values with NaN to match index length
        pad = [np.nan] * window
        return (
            pd.Series(pad + hedge_ratios, index=df.index),
            pd.Series(pad + spreads, index=df.index)
        )

    @staticmethod
    def calculate_kalman_hedge_ratio(target, reference, delta=1e-5, vt=1e-3):
        """
        Dynamic Hedge Ratio using a Kalman Filter.
        State: [beta, alpha] (Slope, Intercept)
        Measurement: Target Price (Y)
        Observation Matrix: [Reference Price (X), 1]

        Args:
            target (Series): Y (ETH)
            reference (Series): X (BTC)
            delta: Process noise covariance (allows beta to drift)
            vt: Measurement noise variance
        """
        # Convert to numpy for speed
        y = target.values
        x = reference.values
        n = len(y)

        # State Vector: [beta, alpha]
        state_mean = np.zeros((2, 1))
        # Covariance Matrix (start with high uncertainty)
        P = np.eye(2) * 10.0

        # Process Noise Covariance (Q)
        # We assume beta/alpha follow a random walk: beta_t = beta_{t-1} + noise
        Q = np.eye(2) * delta

        # Measurement Noise Covariance (R)
        R = vt

        betas = np.zeros(n)
        alphas = np.zeros(n)
        spreads = np.zeros(n)

        for t in range(n):
            # 1. Prediction Step (Random Walk -> Prior = Previous Posterior)
            state_pred = state_mean
            P_pred = P + Q

            # 2. Observation Matrix H = [x_t, 1]
            H = np.array([[x[t], 1.0]])

            # 3. Measurement Residual (Innovation)
            # y_t - (beta * x_t + alpha)
            y_pred = H @ state_pred
            error = y[t] - y_pred

            # 4. Update Step
            # Innovation Covariance S = HPH' + R
            S = H @ P_pred @ H.T + R

            # Kalman Gain K = P_pred * H' * inv(S)
            K = P_pred @ H.T @ np.linalg.inv(S)

            # New State Estimate
            state_mean = state_pred + K * error

            # New Covariance Estimate
            P = (np.eye(2) - K @ H) @ P_pred

            # Store results
            betas[t] = state_mean[0, 0]
            alphas[t] = state_mean[1, 0]

            # Spread = Actual - Modelled
            # Use POSTERIOR beta for spread analysis (common in KF pairs trading)
            spreads[t] = error.item()  # The "Innovation" is effectively the de-trended spread

        return (
            pd.Series(betas, index=target.index),
            pd.Series(spreads, index=target.index)
        )

    @staticmethod
    def calculate_zscore(series, window=60):
        """
        Robust rolling Z-Score calculation.
        """
        roll_mean = series.rolling(window=window).mean()
        roll_std = series.rolling(window=window).std()
        return (series - roll_mean) / roll_std

    @staticmethod
    def perform_adf_test(series):
        """
        Augmented Dickey-Fuller test for stationarity.
        Returns: (Statistic, p-value, is_stationary)
        """
        clean_series = series.dropna()
        if len(clean_series) < 30:  # ADF requires sufficient data
            return 0.0, 1.0, False

        result = adfuller(clean_series)
        stat = result[0]
        p_value = result[1]
        is_stationary = p_value < 0.05
        return stat, p_value, is_stationary