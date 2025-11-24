"""
validation.py

Applies data quality checks on ETL output and separates records into
clean and quarantine datasets. Validates:
    - Required fields
    - Numeric ranges
    - Domain constraints
    - Structural rules (ingredients/steps)
    - Referential integrity

Now reads from the LATEST ETL version directory under ETL_Output/, e.g.:
    ETL_Output/v3_2025-11-24_10-30-15/

Outputs:
    Validation_Output/clean_*.csv
    Validation_Output/quarantined_*.csv
    Validation_Output/validation_report.json
"""

import os
import json
import pandas as pd
import numpy as np

from utils import get_logger, OUTPUT_DIR

logger = get_logger("DataValidator")

# base ETL folder 
ETL_ROOT_DIR = OUTPUT_DIR
VALIDATION_DIR = "Validation_Output"
REPORT_FILE = "validation_report.json"

os.makedirs(VALIDATION_DIR, exist_ok=True)

# allowed values
VALID_DIFFICULTIES = ["Easy", "Medium", "Hard"]
VALID_TYPES = ["view", "like", "cook_attempt", "rating"]


def get_latest_etl_version_dir(base_dir: str) -> str | None:
    """
    Returns full path to latest ETL version directory under base_dir,
    where version folders are named: vN_YYYY-mm-dd_HH-MM-SS.
    """
    if not os.path.exists(base_dir):
        return None

    candidates = []
    for name in os.listdir(base_dir):
        full_path = os.path.join(base_dir, name)
        if not os.path.isdir(full_path):
            continue
        if not name.startswith("v") or "_" not in name:
            continue
        prefix = name.split("_", 1)[0]  # 'v1'
        try:
            num = int(prefix[1:])
            candidates.append((num, full_path))
        except ValueError:
            continue

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    return candidates[-1][1]


#  individual validators

def validate_recipes(df):
    df.columns = df.columns.str.strip()
    clean_df = df.copy()
    quarantine_rows = []
    report = []

    for _, row in df.iterrows():
        errors = []
        is_valid = True

        if pd.isna(row.get("name")) or str(row.get("name", "")).strip() == "":
            errors.append("Missing or empty recipe name")
            is_valid = False
        if pd.isna(row.get("description")):
            errors.append("Missing recipe description")
            is_valid = False

        if pd.isna(row.get("prep_time_minutes")) or row.get("prep_time_minutes", -1) < 0:
            errors.append("prep_time_minutes must be non-negative")
            is_valid = False
        if pd.isna(row.get("cook_time_minutes")) or row.get("cook_time_minutes", -1) < 0:
            errors.append("cook_time_minutes must be non-negative")
            is_valid = False

        if pd.isna(row.get("servings")) or row.get("servings", 0) <= 0:
            errors.append("servings must be positive")
            is_valid = False

        if str(row.get("difficulty")) not in VALID_DIFFICULTIES:
            errors.append(f"Invalid difficulty value: {row.get('difficulty')}")
            is_valid = False

        record = {
            "recipe_id": row["recipe_id"],
            "is_valid": is_valid,
            "errors": errors
        }
        report.append(record)

        if not is_valid:
            quarantine_rows.append(row)

    quarantine_df = pd.DataFrame(quarantine_rows)
    clean_df = df[~df["recipe_id"].isin(quarantine_df.get("recipe_id", []))]

    quarantine_df.to_csv(os.path.join(VALIDATION_DIR, "quarantined_recipe.csv"), index=False)
    clean_df.to_csv(os.path.join(VALIDATION_DIR, "clean_recipe.csv"), index=False)

    return report, len(clean_df), len(quarantine_df)


def validate_users(df):
    df.columns = df.columns.str.strip()

    if 'user_id' not in df.columns:
        logger.error(f"FATAL: 'user_id' column missing. Columns found: {list(df.columns)}")
        quarantine_df = df.copy()
        quarantine_df.to_csv(os.path.join(VALIDATION_DIR, "quarantined_users.csv"), index=False)
        pd.DataFrame().to_csv(os.path.join(VALIDATION_DIR, "clean_users.csv"), index=False)
        return [], 0, len(df)

    quarantine_rows = []
    report = []

    for _, row in df.iterrows():
        errors = []
        is_valid = True

        if pd.isna(row["user_id"]) or str(row["user_id"]).strip() == "":
            errors.append("Missing user_id")
            is_valid = False

        if pd.isna(row["name"]) or str(row["name"]).strip() == "":
            errors.append("Missing user name")
            is_valid = False

        record = {
            "user_id": row["user_id"],
            "is_valid": is_valid,
            "errors": errors
        }
        report.append(record)

        if not is_valid:
            quarantine_rows.append(row)

    quarantine_df = pd.DataFrame(quarantine_rows)
    clean_df = df[~df["user_id"].isin(quarantine_df.get("user_id", []))]

    quarantine_df.to_csv(os.path.join(VALIDATION_DIR, "quarantined_users.csv"), index=False)
    clean_df.to_csv(os.path.join(VALIDATION_DIR, "clean_users.csv"), index=False)

    return report, len(clean_df), len(quarantine_df)


def validate_ingredients(df):
    df.columns = df.columns.str.strip()
    clean_df = df.copy()
    quarantine_rows = []
    report = []

    for _, row in df.iterrows():
        errors = []
        is_valid = True

        if pd.isna(row.get("recipe_id")):
            errors.append("Missing recipe_id link")
            is_valid = False
        if pd.isna(row.get("name")) or str(row.get("name", "")).strip() == "":
            errors.append("Missing ingredient name")
            is_valid = False

        if pd.isna(row.get("quantity")) or row.get("quantity", 0) <= 0:
            errors.append("Quantity must be positive")
            is_valid = False

        record = {
            "ingredient_id": row["ingredient_id"],
            "is_valid": is_valid,
            "errors": errors
        }
        report.append(record)

        if not is_valid:
            quarantine_rows.append(row)

    quarantine_df = pd.DataFrame(quarantine_rows)
    clean_df = df[~df["ingredient_id"].isin(quarantine_df.get("ingredient_id", []))]

    quarantine_df.to_csv(os.path.join(VALIDATION_DIR, "quarantined_ingredients.csv"), index=False)
    clean_df.to_csv(os.path.join(VALIDATION_DIR, "clean_ingredients.csv"), index=False)

    return report, len(clean_df), len(quarantine_df)


def validate_steps(df):
    df.columns = df.columns.str.strip()
    clean_df = df.copy()
    quarantine_rows = []
    report = []

    for idx, row in df.iterrows():
        errors = []
        is_valid = True

        if pd.isna(row.get("recipe_id")):
            errors.append("Missing recipe_id link")
            is_valid = False
        if pd.isna(row.get("instruction")) or str(row.get("instruction", "")).strip() == "":
            errors.append("Missing step instruction")
            is_valid = False

        if pd.isna(row.get("step_no")) or row.get("step_no", 0) <= 0:
            errors.append("step_no must be positive")
            is_valid = False
        if pd.isna(row.get("duration_minutes")) or row.get("duration_minutes", -1) < 0:
            errors.append("duration_minutes must be non-negative")
            is_valid = False

        record = {
            "recipe_id": row["recipe_id"],
            "step_no": row["step_no"],
            "is_valid": is_valid,
            "errors": errors
        }
        report.append(record)

        if not is_valid:
            quarantine_rows.append(row)

    quarantine_df = pd.DataFrame(quarantine_rows)
    quarantine_indices = quarantine_df.index.tolist()
    clean_df = df[~df.index.isin(quarantine_indices)]

    quarantine_df.to_csv(os.path.join(VALIDATION_DIR, "quarantined_steps.csv"), index=False)
    clean_df.to_csv(os.path.join(VALIDATION_DIR, "clean_steps.csv"), index=False)

    return report, len(clean_df), len(quarantine_rows)


def validate_interactions(df):
    df.columns = df.columns.str.strip()
    clean_df = df.copy()
    quarantine_rows = []
    report = []

    for _, row in df.iterrows():
        errors = []
        is_valid = True

        if pd.isna(row.get("user_id")):
            errors.append("Missing user_id link")
            is_valid = False
        if pd.isna(row.get("recipe_id")):
            errors.append("Missing recipe_id link")
            is_valid = False

        if str(row.get("type")) not in VALID_TYPES:
            errors.append(f"Invalid interaction type: {row.get('type')}")
            is_valid = False

        if row.get("type") == "rating":
            if pd.isna(row.get("rating")) or row.get("rating", 0) < 1 or row.get("rating", 6) > 5:
                errors.append(f"Rating out of bounds or missing for rating type: {row.get('rating')}")
                is_valid = False

        if pd.isna(row.get("timestamp")):
            errors.append("Missing timestamp")
            is_valid = False

        record = {
            "interaction_id": row["interaction_id"],
            "is_valid": is_valid,
            "errors": errors
        }
        report.append(record)

        if not is_valid:
            quarantine_rows.append(row)

    quarantine_df = pd.DataFrame(quarantine_rows)
    clean_df = df[~df["interaction_id"].isin(quarantine_df.get("interaction_id", []))]

    quarantine_df.to_csv(os.path.join(VALIDATION_DIR, "quarantined_interactions.csv"), index=False)
    clean_df.to_csv(os.path.join(VALIDATION_DIR, "clean_interactions.csv"), index=False)

    return report, len(clean_df), len(quarantine_df)


#  main validation entry

def main():
    logger.info("Starting Validation Pipeline...")

    latest_etl_dir = get_latest_etl_version_dir(ETL_ROOT_DIR)
    if not latest_etl_dir:
        logger.error(f"No ETL version folders found under {ETL_ROOT_DIR}. Run ETL first.")
        return

    logger.info(f"Using ETL data from latest version folder: {os.path.basename(latest_etl_dir)}")

    # load CSVs from latest ETL version folder
    recipe_df = pd.read_csv(os.path.join(latest_etl_dir, "recipe.csv"))
    ingredients_df = pd.read_csv(os.path.join(latest_etl_dir, "ingredients.csv"))
    steps_df = pd.read_csv(os.path.join(latest_etl_dir, "steps.csv"))
    interactions_df = pd.read_csv(os.path.join(latest_etl_dir, "interactions.csv"))
    users_df = pd.read_csv(os.path.join(latest_etl_dir, "users.csv"))

    report_data = {}

    recipe_report, r_clean, r_quar = validate_recipes(recipe_df)
    report_data["recipes"] = recipe_report
    logger.info(f"[recipes] Processed. Clean: {r_clean} | Quarantined: {r_quar}")

    users_report, u_clean, u_quar = validate_users(users_df)
    report_data["users"] = users_report
    logger.info(f"[users] Processed. Clean: {u_clean} | Quarantined: {u_quar}")

    ing_report, i_clean, i_quar = validate_ingredients(ingredients_df)
    report_data["ingredients"] = ing_report
    logger.info(f"[ingredients] Processed. Clean: {i_clean} | Quarantined: {i_quar}")

    steps_report, s_clean, s_quar = validate_steps(steps_df)
    report_data["steps"] = steps_report
    logger.info(f"[steps] Processed. Clean: {s_clean} | Quarantined: {s_quar}")

    inter_report, inter_clean, inter_quar = validate_interactions(interactions_df)
    report_data["interactions"] = inter_report
    logger.info(f"[interactions] Processed. Clean: {inter_clean} | Quarantined: {inter_quar}")

    report_path = os.path.join(VALIDATION_DIR, REPORT_FILE)
    with open(report_path, "w") as f:
        json.dump(report_data, f, indent=2)

    logger.info(f"Validation Complete. Clean data ready in {VALIDATION_DIR}")


if __name__ == "__main__":
    main()
