"""
train_model.py
---------------

This module trains a simple forecasting model on scraped
smartphone sales data and derives actionable insights such as
optimal launch windows and price points. It uses Prophet for
time‑series forecasting and scikit‑learn for a rudimentary
price‑elasticity model.

Usage::

    python train_model.py --csv ../data/products.csv --brand "Xiaomi" --model "Redmi Note 13"

The script loads historical data, aggregates daily sales counts per
product, fits Prophet to forecast the next 30 days, and
fits a linear regression between price and sales to estimate
optimal price.
"""

import argparse
import logging
import json
from pathlib import Path
import pandas as pd
try:
    from prophet import Prophet  # type: ignore
except ImportError:
    Prophet = None  # fallback implemented below
from sklearn.linear_model import LinearRegression
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def load_data(csv_path: str, brand: str, model_name: str) -> pd.DataFrame:
    """Load and filter scraped data by brand and model.

    Args:
        csv_path: Path to CSV containing scraped product data.
        brand: Target brand to analyse.
        model_name: Substring to match in product names.

    Returns:
        DataFrame filtered on brand and model.
    """
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    df['specs'] = df['specs'].apply(lambda x: json.loads(x))
    df['brand'] = df['specs'].apply(lambda s: s.get('brand', '').lower())
    # Normalize names for matching
    df['name_lower'] = df['name'].str.lower()
    filtered = df[(df['brand'] == brand.lower()) & (df['name_lower'].str.contains(model_name.lower()))]
    return filtered


def prepare_sales_timeseries(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sales count per day to build a time series.

    If `sales` values are missing, we approximate by counting
    occurrences of listings on that day (assuming each listing
    indicates at least one unit sold). This is a rough proxy when
    sales numbers are unavailable.
    """
    df['date'] = df['timestamp'].dt.date
    # Use provided sales counts or fallback to 1 per record
    df['sale_count'] = df['sales'].fillna(1)
    ts = df.groupby('date')['sale_count'].sum().reset_index()
    ts.columns = ['ds', 'y']  # Prophet expects columns named ds (date) and y (target)
    return ts


def forecast_sales(ts: pd.DataFrame, periods: int = 30) -> pd.DataFrame:
    """Fit Prophet and forecast sales for the specified number of days."""
    if Prophet is None:
        # Fallback: naive forecast using rolling mean
        ts_copy = ts.copy()
        ts_copy['yhat'] = ts_copy['y'].rolling(window=7, min_periods=1).mean()
        last_date = ts_copy['ds'].max()
        future_dates = [last_date + pd.Timedelta(days=i) for i in range(1, periods + 1)]
        # Use last rolling mean as forecast for all future periods
        last_mean = ts_copy['yhat'].iloc[-1]
        future_df = pd.DataFrame({'ds': future_dates, 'yhat': [last_mean] * periods})
        return pd.concat([ts_copy[['ds', 'yhat']], future_df], ignore_index=True)
    else:
        m = Prophet(daily_seasonality=True)
        m.fit(ts)
        future = m.make_future_dataframe(periods=periods)
        forecast = m.predict(future)
        return forecast[['ds', 'yhat']]


def find_optimal_launch_window(forecast_df: pd.DataFrame, competitors_df: pd.DataFrame) -> str:
    """Identify the date within the forecast horizon when competitor sales are lowest.

    Args:
        forecast_df: Forecasted sales for the target model (unused but kept for extension).
        competitors_df: DataFrame with aggregated competitor daily sales (columns ds and y).

    Returns:
        Date string representing the optimal launch window.
    """
    # Forecast competitor sales using Prophet for the next 30 days
    if Prophet is None:
        # Fallback: use historical mean as forecast; choose day with lowest competitor average (first day)
        # Create naive forecast
        rolling_mean = competitors_df['y'].rolling(window=7, min_periods=1).mean()
        last_mean = rolling_mean.iloc[-1]
        future_dates = [competitors_df['ds'].max() + pd.Timedelta(days=i) for i in range(1, 31)]
        # All future days have same predicted value; choose the first as optimal launch window
        return future_dates[0].strftime('%Y-%m-%d')
    else:
        comp_model = Prophet(daily_seasonality=True)
        comp_model.fit(competitors_df)
        future = comp_model.make_future_dataframe(periods=30)
        comp_forecast = comp_model.predict(future)
        horizon = comp_forecast.tail(30)
        optimal_row = horizon.loc[horizon['yhat'].idxmin()]
        return optimal_row['ds'].strftime('%Y-%m-%d')


def estimate_optimal_price(df: pd.DataFrame) -> float:
    """Fit a simple linear regression between price and sales to find the revenue‑maximizing price.

    We calculate revenue as price * predicted demand. The optimal price is where
    marginal revenue is zero (derivative of revenue w.r.t price = 0). For a
    linear demand curve `sales = a + b*price`, revenue is `R(p) = p*(a + b*p)`.
    The optimum occurs at p = -a/(2*b). If b >= 0, we return the median price
    to avoid division by zero.
    """
    # We need numeric sales counts; fallback to 1 if missing
    df = df.copy()
    df['sales_count'] = df['sales'].fillna(1)
    X = df[['price']].values
    y = df['sales_count'].values
    if len(df) < 2:
        return float(df['price'].median())
    reg = LinearRegression().fit(X, y)
    a = reg.intercept_
    b = reg.coef_[0]
    if b >= 0:
        # Demand increases with price? unrealistic; fallback to current median
        return float(np.median(df['price']))
    optimal_price = -a / (2 * b)
    # Limit to observed price range
    return float(np.clip(optimal_price, df['price'].min(), df['price'].max()))


def main():
    parser = argparse.ArgumentParser(description='Train forecasting model and derive insights.')
    parser.add_argument('--csv', default='../data/products.csv', help='Path to CSV file with product data')
    parser.add_argument('--brand', required=True, help='Brand to analyse (e.g. Xiaomi)')
    parser.add_argument('--model', required=True, help='Model name substring to match (e.g. Redmi Note 13)')
    parser.add_argument('--competitor_brands', nargs='+', default=['Samsung','Oppo','Vivo'], help='List of competitor brands')
    args = parser.parse_args()

    df = load_data(args.csv, args.brand, args.model)
    if df.empty:
        logging.error('No data found for specified brand/model.')
        return
    ts = prepare_sales_timeseries(df)
    forecast_df = forecast_sales(ts, periods=30)
    # Competitor sales aggregation
    all_df = pd.read_csv(args.csv, parse_dates=['timestamp'])
    all_df['specs'] = all_df['specs'].apply(lambda x: json.loads(x))
    all_df['brand'] = all_df['specs'].apply(lambda s: s.get('brand', '').lower())
    comp_df = all_df[all_df['brand'].isin([b.lower() for b in args.competitor_brands])]
    comp_df['date'] = comp_df['timestamp'].dt.date
    comp_df['sales_count'] = comp_df['sales'].fillna(1)
    comp_ts = comp_df.groupby('date')['sales_count'].sum().reset_index()
    comp_ts.columns = ['ds','y']
    optimal_launch = find_optimal_launch_window(forecast_df, comp_ts)
    optimal_price = estimate_optimal_price(df)
    # Save forecast and insights to files
    out_dir = Path('../model_output')
    out_dir.mkdir(exist_ok=True, parents=True)
    forecast_path = out_dir / f'{args.brand}_{args.model.replace(" ", "_")}_forecast.csv'
    forecast_df.to_csv(forecast_path, index=False)
    insights_path = out_dir / f'{args.brand}_{args.model.replace(" ", "_")}_insights.json'
    insights = {
        'optimal_launch_date': optimal_launch,
        'optimal_price': optimal_price,
        'model': args.model,
        'brand': args.brand
    }
    with open(insights_path, 'w', encoding='utf-8') as f:
        json.dump(insights, f, ensure_ascii=False, indent=2)
    logging.info(f'Forecast saved to {forecast_path}')
    logging.info(f'Insights saved to {insights_path}')
    # Print insights for quick access
    print(json.dumps(insights, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()