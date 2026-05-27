"""Train baseline RF (same as train_chrono_split.ipynb) and save for the Flask app."""

import json
from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_READY = REPO_ROOT / "datasets" / "MODEL_READY_DATASET5.csv"
OUT_DIR = Path(__file__).resolve().parent / "models"


def main():
    df = pd.read_csv(MODEL_READY)
    target_col = "target_fuel_price_tomorrow"
    split_idx = int(len(df) * 0.8)
    train_df = df.iloc[:split_idx]

    X_train = train_df.drop(columns=[target_col])
    y_train = train_df[target_col]
    feature_columns = list(X_train.columns)

    model = RandomForestRegressor(
        n_estimators=300,
        max_depth=7,
        min_samples_split=2,
        min_samples_leaf=30,
        max_features=0.6,
        bootstrap=True,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    artifact = {
        "model": model,
        "feature_columns": feature_columns,
        "target_col": target_col,
    }
    out_path = OUT_DIR / "baseline_rf.joblib"
    joblib.dump(artifact, out_path)
    (OUT_DIR / "feature_columns.json").write_text(json.dumps(feature_columns, indent=2))
    print(f"Saved {out_path} ({len(feature_columns)} features)")


if __name__ == "__main__":
    main()
