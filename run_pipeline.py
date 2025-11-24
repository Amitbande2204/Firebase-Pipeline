"""
run_pipeline.py

Orchestrates the complete end-to-end data pipeline by executing:
    1. (Optional) Firestore seeding
    2. ETL (extract & transform)
    3. Data validation
    4. Analytics generation

Provides structured logging for each stage and ensures sequential,
deterministic pipeline execution.

Usage:
    python run_pipeline.py
        → Full pipeline INCLUDING Firestore seeding (first time).

    python run_pipeline.py --no-seed
        → Skip Firestore seeding, start from ETL stage
           (useful when Firestore is already populated and you are
            just fixing ETL / validation / analytics).
"""

import time
import sys

import firestore_setup
import etl_export_transform
import validation
import analytics
from utils import get_logger

logger = get_logger("Orchestrator")


def run(seed_firestore: bool = True):
    print("\n" + "=" * 50)
    print("   STARTING RECIPE DATA PIPELINE")
    print("=" * 50 + "\n")

    start_time = time.time()

    # step 1: optional seed data
    if seed_firestore:
        logger.info(">>> [STEP 1/4] SEEDING FIRESTORE")
        firestore_setup.main()
        print("-" * 30)
    else:
        logger.info(">>> [STEP 1/4] SKIPPING FIRESTORE SEED (seed_firestore=False)")
        print("[STEP 1/4] Skipping Firestore setup (reusing existing data)...")
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
    print("\n" + "=" * 50)
    print(f"   PIPELINE SUCCESSFUL ({elapsed:.2f}s)")
    print("=" * 50)
    print("1. Clean Data:     Validation_Output/")
    print("2. Insights JSON:  Analytics_Output/analytics_report.json")
    print("3. Visualizations: Analytics_Output/Charts/")


if __name__ == "__main__":
    # simple CLI:
    #   --no-seed → skip Firestore seeding
    seed = True
    if len(sys.argv) > 1 and sys.argv[1] == "--no-seed":
        seed = False

    run(seed_firestore=seed)
