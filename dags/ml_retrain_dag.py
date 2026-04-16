"""
ML Retrain DAG — runs weekly on Sunday at midnight.

Retrains both the Prophet+XGBoost forecaster and the buy-now classifier
on the latest 90 days of mart_price_history data.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)


def retrain_forecaster(**context: dict) -> None:
    from ml.forecaster import PriceForecaster

    forecaster = PriceForecaster()
    metrics = forecaster.train(lookback_days=90)
    logger.info("Forecaster retrain complete. Metrics: %s", metrics)


def retrain_classifier(**context: dict) -> None:
    from ml.classifier import BuyNowClassifier

    clf = BuyNowClassifier()
    metrics = clf.train(lookback_days=90)
    logger.info("Classifier retrain complete. Metrics: %s", metrics)
    # Write fresh buy signals with the newly trained model
    clf.run_inference_and_write()


def evaluate_models(**context: dict) -> None:
    from ml.evaluate import evaluate_and_log

    evaluate_and_log()


default_args = {
    "owner": "shelfsense",
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
}

with DAG(
    dag_id="ml_retrain",
    description="Weekly retrain of Prophet forecaster + XGBoost buy-now classifier",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 0 * * 0",  # Sunday midnight
    default_args=default_args,
    catchup=False,
    tags=["shelfsense", "ml"],
) as dag:

    retrain_fc = PythonOperator(task_id="retrain_forecaster", python_callable=retrain_forecaster)
    retrain_clf = PythonOperator(task_id="retrain_classifier", python_callable=retrain_classifier)
    evaluate = PythonOperator(task_id="evaluate_models", python_callable=evaluate_models)

    [retrain_fc, retrain_clf] >> evaluate
