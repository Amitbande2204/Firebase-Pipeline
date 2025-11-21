"""
run_pipeline.py

Orchestrates the complete end-to-end data pipeline by executing:
    1. Firestore seeding
    2. ETL (extract & transform)
    3. Data validation
    4. Analytics generation

Provides structured logging for each stage and ensures sequential,
deterministic pipeline execution.
"""

import time
import firestore_setup
import etl_export_transform
import validation
import analytics
from utils import get_logger

logger = get_logger("Orchestrator")

def run():
    print("\n" + "="*50)
    print("   STARTING RECIPE DATA PIPELINE")
    print("="*50 + "\n")

    start_time = time.time()

    # step 1: seed data
    logger.info(">>> [STEP 1/4] SEEDING FIRESTORE")
    firestore_setup.main()
    print("-" * 30)

    # step 2: extract & transform
    logger.info(">>> [STEP 2/4] ETL (EXTRACT & TRANSFORM)")
    etl_export_transform.main()
    print("-" * 30)

    # step 3: validation (quality gate)
    logger.info(">>> [STEP 3/4] DATA VALIDATION & QUARANTINE")
    validation.main()
    print("-" * 30)

    # step 4: analytics & visualization
    logger.info(">>> [STEP 4/4] ANALYTICS GENERATION")
    analytics.main()

    elapsed = time.time() - start_time
    print("\n" + "="*50)
    print(f"   PIPELINE SUCCESSFUL ({elapsed:.2f}s)")
    print("="*50)
    print("1. Clean Data:     Validation_Output/")
    print("2. Insights JSON:  Analytics_Output/analytics_report.json")
    print("3. Visualizations: Analytics_Output/Charts/")

if __name__ == "__main__":
    run()