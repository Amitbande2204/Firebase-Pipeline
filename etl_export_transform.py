"""
etl_export_transform.py

Extracts data from Firestore, normalizes nested recipe, user, and interaction
documents, and exports them as structured CSV files for downstream validation
and analytics. Handles:
    - Firestore collection streaming (recipes, users, interactions)
    - Flattening of ingredients and steps
    - Creation of recipe.csv, ingredients.csv, steps.csv, users.csv, interactions.csv

Acts as the ETL stage of the pipeline and writes all outputs to ETL_Output/.
"""

import firebase_admin
from firebase_admin import credentials, firestore
import json
import os
import pandas as pd
from utils import (
    SERVICE_ACCOUNT_PATH, PROJECT_ID, OUTPUT_DIR, get_logger
)

logger = get_logger("ETL_Pipeline")

def fetch_firestore_data():
    """Extracts raw collections from Firestore."""
    # check if app is already initialized to avoid errors
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})

    db = firestore.client()
    logger.info("Fetching collections from Firestore...")
    
    # stream data to avoid memory issues with large datasets
    return (
        [doc.to_dict() for doc in db.collection("recipes").stream()],
        [doc.to_dict() for doc in db.collection("users").stream()], 
        [doc.to_dict() for doc in db.collection("interactions").stream()]
    )

def normalize_and_save(recipes, users, interactions):
    """Transforms nested JSON into flat, normalized CSV tables."""
    logger.info("Normalizing data...")

    recipe_rows = []
    ingredients_rows = []
    steps_rows = []
    
    # --- 1. process recipes, ingredients, steps ---
    for r in recipes:
        # flatten recipe table
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
        
        # flatten ingredients table
        for i, ing in enumerate(r.get("ingredients", [])):
            ingredients_rows.append({
                "ingredient_id": str(ing.get("ingredient_id")),
                "recipe_id": str(r.get("recipe_id")),
                "name": str(ing.get("name")),
                "quantity": ing.get("quantity", 0),
                "unit": str(ing.get("unit", ""))
            })

        # flatten steps table
        for i, st in enumerate(r.get("steps", [])):
            steps_rows.append({
                "recipe_id": str(r.get("recipe_id")),
                "step_no": int(st.get("step_no", 0)),
                "instruction": str(st.get("instruction", "")),
                "duration_minutes": int(st.get("duration_minutes", 0))
            })

    #  2. process interactions 
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

    #  3. process users 
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

    # save to CSV
    logger.info(f"Saving CSVs to {OUTPUT_DIR}...")
    
    pd.DataFrame(recipe_rows).to_csv(os.path.join(OUTPUT_DIR, "recipe.csv"), index=False)
    pd.DataFrame(ingredients_rows).to_csv(os.path.join(OUTPUT_DIR, "ingredients.csv"), index=False)
    pd.DataFrame(steps_rows).to_csv(os.path.join(OUTPUT_DIR, "steps.csv"), index=False)
    pd.DataFrame(interactions_rows).to_csv(os.path.join(OUTPUT_DIR, "interactions.csv"), index=False)
    pd.DataFrame(users_rows).to_csv(os.path.join(OUTPUT_DIR, "users.csv"), index=False) 

#  main function
def main():
    try:
        # fetch data
        recipes, users, interactions = fetch_firestore_data()
            
        # transform and save CSVs (including the missing users.csv)
        normalize_and_save(recipes, users, interactions)
        logger.info("ETL Complete.")
        
    except Exception as e:
        logger.error(f"ETL Failed: {e}")
        raise

if __name__ == "__main__":
    main()