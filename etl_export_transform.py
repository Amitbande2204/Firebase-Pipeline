"""
etl_export_transform.py

Extracts data from Firestore, normalizes nested recipe, user, and interaction
documents, and exports them as structured CSV files for downstream validation
and analytics.

Features:
    - Full extract on first run.
    - Incremental extract on subsequent runs using an 'updated_at' field.
    - Versioned ETL output folders (v1_YYYY-mm-dd_HH-MM-SS, v2_..., etc).
    - Backup of previous ETL versions under ETL_Output/Backup/.

Collections handled:
    - recipes
    - users
    - interactions

Outputs (per run, in a new version folder):
    ETL_Output/vN_YYYY-mm-dd_HH-MM-SS/
        recipe.csv
        ingredients.csv
        steps.csv
        users.csv
        interactions.csv
"""

import os
import shutil
import datetime
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore

from utils import (
    SERVICE_ACCOUNT_PATH,
    PROJECT_ID,
    OUTPUT_DIR,
    get_logger,
)

logger = get_logger("ETL_Pipeline")

# checkpoint file to track last successful ETL time (UTC)
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "etl_checkpoint.txt")


#  helpers- checkpoint, firestore init, version discovery
def get_last_run_timestamp():
    """Reads last ETL run timestamp (UTC) from checkpoint file, if present."""
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            value = f.read().strip()
        if not value:
            return None
        return datetime.datetime.fromisoformat(value)
    except Exception as e:
        logger.warning(f"Failed to parse checkpoint file, running full extract. Error: {e}")
        return None


def save_last_run_timestamp(ts: datetime.datetime):
    """Persists ETL checkpoint timestamp as ISO string."""
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(ts.isoformat())
        logger.info(f"ETL checkpoint updated to {ts.isoformat()}")
    except Exception as e:
        logger.error(f"Failed to write checkpoint file: {e}")


def init_firestore():
    """Initializes Firebase app if not already initialized."""
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})
    return firestore.client()


def _list_version_dirs():
    """Returns list of (version_number:int, full_path:str) for ETL_Output/vN_* dirs."""
    if not os.path.exists(OUTPUT_DIR):
        return []

    results = []
    for name in os.listdir(OUTPUT_DIR):
        full_path = os.path.join(OUTPUT_DIR, name)
        if not os.path.isdir(full_path):
            continue
        if not name.startswith("v") or "_" not in name:
            # ignore non-version folders, e.g. Backup, etc.
            continue
        prefix = name.split("_", 1)[0]  # 'v1'
        try:
            num = int(prefix[1:])
            results.append((num, full_path))
        except ValueError:
            continue
    return results


def get_latest_version_dir():
    """Returns the latest version directory path under OUTPUT_DIR, or None."""
    versions = _list_version_dirs()
    if not versions:
        return None
    versions.sort(key=lambda x: x[0])  # sort by version number
    return versions[-1][1]  # path of highest version


def create_new_version_dir():
    """
    Creates a new ETL version directory:
        v{N}_{timestamp}

    Returns:
        (new_version_path, new_version_name, new_version_number)
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    versions = _list_version_dirs()
    if versions:
        max_num = max(v[0] for v in versions)
    else:
        max_num = 0

    new_num = max_num + 1
    ts_str = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    folder_name = f"v{new_num}_{ts_str}"
    full_path = os.path.join(OUTPUT_DIR, folder_name)

    os.makedirs(full_path, exist_ok=False)
    logger.info(f"Created new ETL output folder: {folder_name}")
    return full_path, folder_name, new_num


def backup_previous_version(prev_path: str | None):
    """
    Copies the previous version directory into:
        ETL_Output/Backup/<same_folder_name>/

    Does nothing if prev_path is None.
    """
    if not prev_path:
        return

    backup_root = os.path.join(OUTPUT_DIR, "Backup")
    os.makedirs(backup_root, exist_ok=True)

    name = os.path.basename(prev_path)
    dest = os.path.join(backup_root, name)

    if os.path.exists(dest):
        logger.info(f"Backup for {name} already exists, skipping backup copy.")
        return

    try:
        shutil.copytree(prev_path, dest)
        logger.info(f"Backed up previous ETL version to: Backup/{name}")
    except Exception as e:
        logger.warning(f"Failed to backup previous version {name}: {e}")


#  Firestore fetch (full or incremental)
def fetch_firestore_data(last_run_ts=None):
    """
    Extracts raw collections from Firestore.

    If last_run_ts is None:
        - Full extract (all documents).
    Else:
        - Incremental extract (documents where updated_at > last_run_ts).
    """
    db = init_firestore()
    logger.info("Fetching collections from Firestore...")

    if last_run_ts is None:
        logger.info("No checkpoint found → FULL extract from Firestore.")
        recipes_ref = db.collection("recipes")
        users_ref = db.collection("users")
        interactions_ref = db.collection("interactions")
    else:
        logger.info(
            f"Checkpoint found ({last_run_ts.isoformat()}) → INCREMENTAL extract (updated_at > checkpoint)."
        )
        recipes_ref = db.collection("recipes").where("updated_at", ">", last_run_ts)
        users_ref = db.collection("users").where("updated_at", ">", last_run_ts)
        interactions_ref = db.collection("interactions").where("updated_at", ">", last_run_ts)

    recipes = [doc.to_dict() for doc in recipes_ref.stream()]
    users = [doc.to_dict() for doc in users_ref.stream()]
    interactions = [doc.to_dict() for doc in interactions_ref.stream()]

    logger.info(
        f"Fetched from Firestore → recipes: {len(recipes)}, users: {len(users)}, interactions: {len(interactions)}"
    )

    return recipes, users, interactions


def merge_with_existing(df_new: pd.DataFrame,
                        prev_dir: str | None,
                        filename: str,
                        key_columns,
                        incremental: bool) -> pd.DataFrame:
    """
    Merges new data with existing CSV from previous ETL version, if any.

    - If incremental=True and prev_dir exists and file exists:
        - Load previous CSV.
        - Append new rows.
        - Drop duplicates on key_columns (keep last).
    - Else:
        - Return df_new as is.
    """
    if incremental and prev_dir:
        existing_path = os.path.join(prev_dir, filename)
        if os.path.exists(existing_path):
            try:
                df_existing = pd.read_csv(existing_path)
                combined = pd.concat([df_existing, df_new], ignore_index=True)
                if key_columns:
                    combined = combined.drop_duplicates(subset=key_columns, keep="last")
                logger.info(f"Merged incremental rows into {filename} from previous version.")
                return combined
            except Exception as e:
                logger.warning(f"Failed to merge with existing {filename}, using only new data. Error: {e}")
                return df_new
    return df_new


#  normalization & saving (per-version)

def normalize_and_save(recipes,
                       users,
                       interactions,
                       incremental: bool,
                       prev_version_dir: str | None,
                       new_version_dir: str):
    """
    Transforms nested JSON into flat, normalized CSV tables, merges with
    previous ETL snapshot (if incremental), and writes new snapshot into
    a fresh version directory.
    """
    logger.info("Normalizing data...")

    recipe_rows = []
    ingredients_rows = []
    steps_rows = []

    #  process recipes, ingredients, steps ---
    for r in recipes:
        recipe_rows.append({
            "recipe_id": str(r.get("recipe_id")),
            "name": str(r.get("name")),
            "description": str(r.get("description", "")),
            "prep_time_minutes": r.get("prep_time_minutes", 0),
            "cook_time_minutes": r.get("cook_time_minutes", 0),
            "difficulty": str(r.get("difficulty", "Unknown")),
            "servings": r.get("servings", 1),
            "tags": "|".join(r.get("tags", [])),
            "cuisines": "|".join(r.get("cuisines", []))
        })

        for ing in r.get("ingredients", []):
            ingredients_rows.append({
                "ingredient_id": str(ing.get("ingredient_id")),
                "recipe_id": str(r.get("recipe_id")),
                "name": str(ing.get("name")),
                "quantity": ing.get("quantity", 0),
                "unit": str(ing.get("unit", ""))
            })

        for st in r.get("steps", []):
            steps_rows.append({
                "recipe_id": str(r.get("recipe_id")),
                "step_no": int(st.get("step_no", 0)),
                "instruction": str(st.get("instruction", "")),
                "duration_minutes": int(st.get("duration_minutes", 0))
            })

    interactions_rows = []
    for it in interactions:
        interactions_rows.append({
            "interaction_id": str(it.get("interaction_id")),
            "user_id": str(it.get("user_id")),
            "recipe_id": str(it.get("recipe_id")),
            "type": str(it.get("type")),
            "rating": float(it.get("rating")) if it.get("rating") else None,
            "like": bool(it.get("like")) if it.get("like") is not None else False,
            "timestamp": str(it.get("timestamp"))
        })

    users_rows = []
    for u in users:
        users_rows.append({
            "user_id": str(u.get("user_id")),
            "name": str(u.get("name")),
            "city": str(u.get("city", "Unknown")),
            "state": str(u.get("state", "Unknown")),
            "country": str(u.get("country", "Unknown")),
            "email": str(u.get("email", ""))
        })

    recipe_df_new = pd.DataFrame(recipe_rows)
    ing_df_new = pd.DataFrame(ingredients_rows)
    steps_df_new = pd.DataFrame(steps_rows)
    interactions_df_new = pd.DataFrame(interactions_rows)
    users_df_new = pd.DataFrame(users_rows)

    logger.info(
        f"New rows → recipes: {len(recipe_df_new)}, ingredients: {len(ing_df_new)}, "
        f"steps: {len(steps_df_new)}, interactions: {len(interactions_df_new)}, "
        f"users: {len(users_df_new)}"
    )

    os.makedirs(new_version_dir, exist_ok=True)

    # merge with existing (previous version) if incremental, then save
    recipe_df_final = merge_with_existing(
        recipe_df_new, prev_version_dir, "recipe.csv", ["recipe_id"], incremental
    )
    ing_df_final = merge_with_existing(
        ing_df_new, prev_version_dir, "ingredients.csv", ["ingredient_id"], incremental
    )
    steps_df_final = merge_with_existing(
        steps_df_new, prev_version_dir, "steps.csv", ["recipe_id", "step_no"], incremental
    )
    interactions_df_final = merge_with_existing(
        interactions_df_new, prev_version_dir, "interactions.csv", ["interaction_id"], incremental
    )
    users_df_final = merge_with_existing(
        users_df_new, prev_version_dir, "users.csv", ["user_id"], incremental
    )

    recipe_df_final.to_csv(os.path.join(new_version_dir, "recipe.csv"), index=False)
    ing_df_final.to_csv(os.path.join(new_version_dir, "ingredients.csv"), index=False)
    steps_df_final.to_csv(os.path.join(new_version_dir, "steps.csv"), index=False)
    interactions_df_final.to_csv(os.path.join(new_version_dir, "interactions.csv"), index=False)
    users_df_final.to_csv(os.path.join(new_version_dir, "users.csv"), index=False)

    logger.info(f"ETL CSVs written to version folder: {os.path.basename(new_version_dir)}")


#  main ETL entry

def main():
    try:
        last_ts = get_last_run_timestamp()
        prev_version_dir = get_latest_version_dir()

        # backup previous version (if any)
        backup_previous_version(prev_version_dir)

        # prepare new version folder
        new_version_dir, folder_name, version_num = create_new_version_dir()

        incremental = last_ts is not None and prev_version_dir is not None
        if incremental:
            logger.info(f"Running ETL in INCREMENTAL mode → new version: {folder_name}")
        else:
            logger.info(f"Running ETL in FULL mode → new version: {folder_name}")

        # fetch data
        recipes, users, interactions = fetch_firestore_data(last_ts)

        # normalize and save (per-version)
        normalize_and_save(
            recipes=recipes,
            users=users,
            interactions=interactions,
            incremental=incremental,
            prev_version_dir=prev_version_dir,
            new_version_dir=new_version_dir,
        )

        # update checkpoint to NOW
        save_last_run_timestamp(datetime.datetime.utcnow())
        logger.info("ETL normalization + versioned export complete.")

    except Exception as e:
        logger.error(f"ETL Failed: {e}")
        raise


if __name__ == "__main__":
    main()
