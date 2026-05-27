"""Load baseline RF and predict tomorrow's fuel price from feature rows."""

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

_MODEL_DIR = Path(__file__).resolve().parent / "models"
_MODEL_PATH = _MODEL_DIR / "baseline_rf.joblib"
_FEATURE_COLUMNS_PATH = _MODEL_DIR / "feature_columns.json"

_artifact = None
_feature_columns = None


def _load_artifact():
    global _artifact, _feature_columns
    if _artifact is not None:
        return _artifact

    if not _MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {_MODEL_PATH}. "
            "Run: python app/backend/export_rf_model.py"
        )

    _artifact = joblib.load(_MODEL_PATH)
    _feature_columns = _artifact.get("feature_columns")
    if _feature_columns is None and _FEATURE_COLUMNS_PATH.exists():
        _feature_columns = json.loads(_FEATURE_COLUMNS_PATH.read_text())
    if not _feature_columns:
        raise ValueError("feature_columns missing from model artifact")
    return _artifact


def feature_column_names():
    _load_artifact()
    return list(_feature_columns)


def build_placeholder_features(
    *,
    postcode: str | int | None,
    current_price: float,
    location_key: str = "",
    forecast_date: datetime | None = None,
    prev_price: float | None = None,
) -> pd.DataFrame:
    """
    Build one feature row with placeholder values (replace with real data later).

    Uses current_price for fuel-related fields; random but stable values for the rest.
    forecast_date: calendar day the model treats as "today" when predicting tomorrow.
    prev_price: yesterday's price (for recursive multi-day forecasts).
    """
    cols = feature_column_names()
    seed_key = f"{location_key}|{forecast_date.date().isoformat() if forecast_date else 'now'}"
    rng = random.Random(seed_key or str(postcode or "default"))

    try:
        pc = int(str(postcode).strip()) if postcode else 2000
    except ValueError:
        pc = 2000

    when = forecast_date or datetime.now(timezone.utc)
    dow = when.weekday()
    day_sin = float(np.sin(2 * np.pi * dow / 7))
    day_cos = float(np.cos(2 * np.pi * dow / 7))
    is_hike = int(dow == 2)  # placeholder: treat Wednesday as hike day

    lag_1 = prev_price if prev_price is not None else current_price + rng.uniform(-3, 3)
    price_change_24h = current_price - lag_1

    # TODO: replace placeholders with live macro/weather/postcode history features.
    values = {
        "postcode": pc,
        "fuel_price": current_price,
        "temp_max": rng.uniform(18, 32),
        "temp_min": rng.uniform(8, 18),
        "rainfall": rng.uniform(0, 15),
        "oil_price": rng.uniform(75, 95),
        "tgp_sydney": rng.uniform(140, 175),
        "aud_usd": rng.uniform(0.62, 0.68),
        "fuel_postcode_daily_avg": current_price,
        "fuel_postcode_price_lag_1": lag_1,
        "fuel_postcode_rolling_7d": current_price + rng.uniform(-2, 2),
        "tgp_sydney_lag_1": rng.uniform(140, 175),
        "aud_usd_lag_1": rng.uniform(0.62, 0.68),
        "oil_rolling_7d": rng.uniform(75, 95),
        "tgp_rolling_7d": rng.uniform(140, 175),
        "aud_usd_rolling_7d": rng.uniform(0.62, 0.68),
        "oil_price_lag_19": rng.uniform(75, 95),
        "oil_price_lag_21": rng.uniform(75, 95),
        "oil_price_lag_19_to_22_mean": rng.uniform(75, 95),
        "retail_margin": rng.uniform(10, 25),
        "price_change_24h": price_change_24h,
        "day_of_week": dow,
        "day_sin": day_sin,
        "day_cos": day_cos,
        "is_hike_day": is_hike,
        "margin_hike_interaction": rng.uniform(0, 50),
    }

    row = {c: values.get(c, rng.uniform(0, 1)) for c in cols}
    return pd.DataFrame([row], columns=cols)


def predict_tomorrow_price(
    *,
    postcode: str | int | None,
    current_price: float,
    location_key: str = "",
    forecast_date: datetime | None = None,
    prev_price: float | None = None,
) -> float:
    artifact = _load_artifact()
    model = artifact["model"]
    X = build_placeholder_features(
        postcode=postcode,
        current_price=current_price,
        location_key=location_key,
        forecast_date=forecast_date,
        prev_price=prev_price,
    )
    return float(model.predict(X)[0])


def predict_recursive_forecast(
    *,
    postcode: str | int | None,
    current_price: float,
    location_key: str = "",
    horizon_days: int = 14,
    start_date: datetime | None = None,
) -> list[dict]:
    """
    Chain single-day predictions: each day's forecast becomes the next day's input price.
    Returns today (actual) plus horizon_days forward predictions.
    """
    artifact = _load_artifact()
    model = artifact["model"]
    start = (start_date or datetime.now(timezone.utc)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    series = [
        {
            "date": start.date().isoformat(),
            "day_offset": 0,
            "price": round(float(current_price), 2),
            "kind": "actual",
        }
    ]

    price = float(current_price)
    prev_price = float(current_price)

    for step in range(1, horizon_days + 1):
        forecast_day = start + timedelta(days=step - 1)
        X = build_placeholder_features(
            postcode=postcode,
            current_price=price,
            location_key=location_key,
            forecast_date=forecast_day,
            prev_price=prev_price,
        )
        next_price = float(model.predict(X)[0])
        target_day = start + timedelta(days=step)
        series.append(
            {
                "date": target_day.date().isoformat(),
                "day_offset": step,
                "price": round(next_price, 2),
                "kind": "forecast",
            }
        )
        prev_price = price
        price = next_price

    return series


def generate_mock_backtest(
    *,
    location_key: str,
    anchor_price: float,
    days: int = 7,
) -> dict:
    """
    Mock "how did we do?" history: for each past day, predicted vs actual when it arrived.

    Deterministic per location. Replace with real logged predictions later.
    """
    rng = random.Random(f"{location_key}|backtest")
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # Random-walk actuals ending near anchor_price (most recent day ≈ yesterday)
    actuals = [float(anchor_price)]
    for _ in range(days - 1):
        actuals.insert(0, actuals[0] + rng.uniform(-3.5, 3.5))

    series = []
    abs_errors = []
    for i in range(days):
        day = today - timedelta(days=days - i)
        actual = actuals[i]
        error = rng.uniform(-4.5, 4.5)
        predicted = actual + error
        abs_err = abs(error)
        abs_errors.append(abs_err)
        series.append(
            {
                "date": day.date().isoformat(),
                "day_offset": i - days,
                "actual": round(actual, 2),
                "predicted": round(predicted, 2),
                "error": round(error, 2),
                "abs_error": round(abs_err, 2),
            }
        )

    mae = sum(abs_errors) / len(abs_errors) if abs_errors else 0.0
    return {
        "days": days,
        "mae": round(mae, 2),
        "series": series,
    }
