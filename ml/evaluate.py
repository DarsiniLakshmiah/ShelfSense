"""
Model evaluation + metrics logging — ml/evaluate.py

Runs backtests for forecaster and classifier, logs metrics to Snowflake
(or stdout if table doesn't exist yet).

Usage:
    evaluate_and_log()
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def evaluate_and_log() -> dict[str, Any]:
    """Run model evaluations and log results."""
    from ml.classifier import BuyNowClassifier
    from ml.forecaster import PriceForecaster

    results: dict[str, Any] = {"evaluated_at": datetime.utcnow().isoformat()}

    # --- Forecaster ---
    try:
        forecaster = PriceForecaster()
        fc_metrics = forecaster.train(lookback_days=90)
        results["forecaster"] = fc_metrics
        logger.info("Forecaster metrics: %s", fc_metrics)
    except Exception:
        logger.exception("Forecaster evaluation failed")
        results["forecaster"] = {"error": "evaluation failed"}

    # --- Classifier ---
    try:
        clf = BuyNowClassifier()
        clf_metrics = clf.train(lookback_days=90)
        results["classifier"] = clf_metrics
        logger.info("Classifier metrics: %s", clf_metrics)
    except Exception:
        logger.exception("Classifier evaluation failed")
        results["classifier"] = {"error": "evaluation failed"}

    # --- Log to console (extend to write to Snowflake/MLflow when ready) ---
    print("=" * 60)
    print("ShelfSense Model Evaluation Report")
    print(f"Evaluated at: {results['evaluated_at']}")
    print("-" * 60)
    for model_name, metrics in results.items():
        if model_name == "evaluated_at":
            continue
        print(f"\n{model_name.upper()}")
        if isinstance(metrics, dict):
            for k, v in metrics.items():
                print(f"  {k}: {v}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    evaluate_and_log()
