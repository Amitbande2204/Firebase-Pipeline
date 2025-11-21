# Firebase-Pipeline

#  Data Engineering Assesment (Firebase-Based Recipe Analytics Pipeline)  
A complete end-to-end **Data Engineering workflow** using **Firestore**, **Python**, **ETL**, **Analytics**, and **Visualization**.

# Overview:
This project implements a complete end‑to‑end analytics pipeline using Firestore as the source system.  
The pipeline seeds Firestore with your recipe (Idli Sambar) along with realistic synthetic data, performs ETL to CSV, validates the exported data, and generates analytics with visual insights.

# Project Structure:
firestore_setup.py       -> Seeds Firestore with recipes, users, interactions + bad data.
etl_export_transform.py  -> Extracts from Firestore, normalizes, exports CSVs.
validation.py            -> Validates exported CSVs, produces clean & quarantine outputs.
analytics.py             -> Generates insights, charts, analytics_report.json.
run_pipeline.py          -> Orchestrates all steps end‑to‑end.
utils.py                 -> Helpers, logging, folder configuration.
requirements.txt         -> Dependencies.

# Data Modeling: 
A well-structured data model is the backbone of this pipeline.  
The model follows **Star Schema principles**, **normalization rules**, and **event-driven design**, ensuring scalability, analytical usability, and consistency.

# Recipes Data Model
# Entity: Recipe
Represents a complete cooking item with all metadata, ingredients, steps, and properties.
| Field             | Type                      | Description                                     |
|-------------------|---------------------------|-------------------------------------------------|
| recipe_id         | STRING (PK)               | Unique normalized ID for recipe, Firestore-safe |
| name              | STRING                    | Recipe name                                     |
| description       | STRING                    | Short description of dish                       |
| prep_time_minutes | INTEGER                   | Time needed for preparation                     |
| cook_time_minutes | INTEGER                   | Time needed for cooking                         |
| difficulty        | STRING (Easy/Medium/Hard) | Used for segmentation and insight creation      |
| servings          | INTEGER                   | Number of servings                              |
| tags              | ARRAY<STRING>             | Category tags (breakfast, spicy, etc.)          |
| cuisines          | ARRAY<STRING>             | Cuisine classification                          |
| ingredients       | ARRAY<OBJECT>             | Embedded ingredient objects                     |
| steps             | ARRAY<OBJECT>             | Embedded cooking steps                          | 

The recipe document acts as the primary fact  ource for all derived tables.

# Ingredients Data Model
Each ingredient is extracted into a normalized table during ETL.
# Entity: Ingredient
| Field         | Type        | Description                  |
|---------------|-------------|------------------------------|
| ingredient_id | STRING (PK) | Unique per ingredient object |
| recipe_id     | STRING (FK) | Links to parent recipe       |
| name          | STRING      | Ingredient name              |
| quantity      | FLOAT       | Quantity used                |
| unit          | STRING      | Measurement unit             |

**Why normalized?**
- Enables ingredient-level analytics  
- Supports insights such as:  
  → most common ingredients,  
  → ingredient engagement,  
  → ingredient cost analysis (future extension)

# Steps Data Model
Captures full cooking flow for each recipe.
# Entity: Step
| Field            | Type        | Description            |
|------------------|-------------|------------------------|
| recipe_id        | STRING (FK) | Links to parent recipe |
| step_no          | INT         | Sequential step number |
| instruction      | STRING      | Text instruction       |
| duration_minutes | INT         | Estimated time         |

Use Cases:
- Calculate total steps per recipe  
- Analyze complexity (Insight #13)  
- Assess time distribution across cooking instructions

# Users Data Model
Represents application users performing actions on recipes.
# Entity: User
| Field   | Type        | Description                          |
|---------|-------------|--------------------------------------|
| user_id | STRING (PK) | Unique ID created via normalize_id() |
| name    | STRING      | User’s full name                     |
| city    | STRING      | City for geospatial analytics        |
| state   | STRING      | State for demographic insights       |
| country | STRING      | Country                              |
| email   | STRING      | Contact email                        |

Use Cases:
- User segmentation  
- Geography heatmaps  
- Personalized recommendation modeling  

# Interactions Data Model
The event log capturing every action performed by a user.
# Entity: Interaction
| Field          | Type               | Description                              |
|----------------|--------------------|------------------------------------------|
| interaction_id | STRING (PK)        | Unique ID for interaction                |
| user_id        | STRING (FK)        | References Users table                   |
| recipe_id      | STRING (FK)        | References Recipes table                 |
| type           | STRING             | One of: view, like, cook_attempt, rating |
| rating         | FLOAT (nullable)   | Present if type=rating                   |
| like           | BOOLEAN (nullable) | Present if type=like                     |
| timestamp      | TIMESTAMP          | Event time                               |

This table forms the Fact Table of the pipeline.
Every insight such as top views, engagement scoring, funnel conversion, and location-based usage comes from this model.

# ERD Representation: 
      ┌──────────────────┐
      │     USERS        │
      │ user_id (PK)     │
      └────────┬─────────┘
               │
               │ 1..N
               │
    ┌──────────▼───────────┐
    │    INTERACTIONS      │
    │ interaction_id (PK)  │
    │ user_id (FK)         │
    │ recipe_id (FK)       │
    └──────────┬───────────┘
               │
               │ N..1
               │
   ┌───────────▼───────────┐
   │       RECIPES         │
   │ recipe_id (PK)        │
   └───────┬───────────────┘
           │
 ┌─────────┴───────────┐
 │                     │
 ▼                     ▼

# Firebase Source Data Setup:
# Firestore Seeding (firestore_setup.py)
# Implements:
- Your recipe **Idli Sambar** as the primary recipe  
- Adds **18 synthetic realistic recipes**
- Adds **30 users** with city/state data  
- Generates **400 interactions**  
- Injects **5 bad records** intentionally for validation testing:
  - Negative prep time  
  - Invalid difficulty  
  - Missing recipe name  
  - Rating > 5  
  - Invalid interaction type  

# ETL/ELT Pipeline:(etl_export_transform.py)  
It is designed to be **deterministic**, **idempotent**, **scalable**, and **audit-friendly**, ensuring that the exported data is reliable for downstream validation and analytics.

# ETL Architecture Overview
The ETL pipeline consists of **three phases**:
1. **Extract** – Stream structured data from Firestore  
2. **Transform** – Normalize nested NoSQL documents into relational tables  
3. **Load** – Persist the normalized datasets as CSVs inside `ETL_Output/`

A controlled and logged workflow ensures that every run produces consistent results.

# Extraction (E)
Extraction is performed using the Firebase Admin SDK.  
Instead of loading entire collections at once, Firestore data is **streamed**, ensuring:

# Extracted Firestore Collections:
- recipes
- users
- interactions

# Code Behavior
- The ETL script first checks if a Firebase app is already initialized to avoid duplicate initialization errors.
- Each collection is streamed using `collection().stream()`.
- Each Firestore document is converted into a Python `dict` using `to_dict()`.

This ensures the raw input is consistent and safely handled before transformations.

# Transformation (T)
The transformation step is the heart of the pipeline.

Since Firestore is a **NoSQL document store**, the recipe data is nested and unstructured.  
The ETL process converts it into **fully normalized relational tables** following 1NF, 2NF, and 3NF principles.

# Transformations performed:
# A. Recipes → recipe.csv
Flatten nested recipe attributes:
- Basic recipe metadata
- Tags flattened into `|` separated strings
- Cuisine array converted to string format

# B. Ingredients → ingredients.csv
Each ingredient from every recipe is extracted with:
- Unique `ingredient_id`  
- Foreign key `recipe_id`  
- Quantity + unit  
- Cleaned ingredient name  

This enables ingredient-level analytics.

# C. Steps → steps.csv
Each step is extracted into:
- recipe_id  
- step_no  
- instruction  
- duration_minutes  

Useful for measuring recipe complexity and step count analytics.

# D. Interactions → interactions.csv
The user event log is normalized with:
- interaction_id  
- recipe_id  
- user_id  
- type (view/like/cook_attempt/rating)  
- optional rating  
- optional like boolean  
- timestamp  

This table powers engagement scoring, funnels, and correlation analytics.

# E. Users → users.csv
All user metadata is exported into:
- user_id  
- name  
- city/state/country  
- email  

Supports segmentation and geo-based analytics.

# Loading (L)
Once transformed, the data is persisted into the structured directory:

# ETL Process Diagram (Textual)

     Firestore
┌───────────────────────┐
│  recipes              │
│  users                │
│  interactions         │
└───────────┬───────────┘
            │ Extract (Stream)
            ▼
    Raw Python Dicts
┌───────────────────────┐
│ Flatten / Normalize   │
│ Split nested arrays   │
│ Validate types        │
└───────────┬───────────┘
            │ Transform
            ▼
 Normalized DataFrames
┌───────────────────────┐
│ recipe.csv            │
│ ingredients.csv       │
│ steps.csv             │
│ interactions.csv      │
│ users.csv             │
└───────────┬───────────┘
            │ Load
            ▼
   ETL_Output/ (Final)

#  Data Quality Validation: (validation.py)
The validation layer acts as the quality checkpoint of the pipeline. After ETL, every dataset is validated against multiple rules—required fields, numeric correctness, domain constraints, structural checks, data types, and referential integrity. Records that pass all rules are stored in clean_*.csv, while records failing any rule (e.g., negative prep time, invalid difficulty, missing recipe names, out-of-range ratings, invalid interaction types, empty ingredients/steps, broken user/recipe links) are isolated into quarantine_*.csv along with the failure reason.

This approach ensures:
- Only accurate and consistent data flows into analytics
- All bad or malformed data is safely captured for debugging
- Analytics outputs remain reliable and free from noise
- Validation is transparent, auditable, and easy to extend
- Outputs include clean CSVs, quarantine CSVs, and a consolidated validation_report.json.
Outputs:
- **clean_*.csv** → stored in Validation_Output/  
- **quarantine_*.csv** → invalid records with reasons  
- **validation_report.json**

#  Analytics Requirements:(analytics.py)
The analytics module processes all validated (clean) data to generate 15 business and behavioral insights, supported by visual charts. These insights cover ingredient usage, recipe complexity, user behavior, and engagement patterns across the dataset.

# Key insights include:
- Top Ingredients – Identifies the most frequently used ingredients across all recipes.
- Preparation Time Trends – Average prep time and time distribution across dishes.
- Difficulty Distribution – Share of Easy, Medium, and Hard recipes.
- Prep Time vs Likes Correlation – Measures relationship between effort and popularity.
- Most Viewed Recipes – Recipes with the highest user views.
- High-Engagement Ingredients – Ingredients common in top-engagement dishes.
- User Funnel Analysis – View → Like → Cook Attempt flow.
- User Segmentation – Low, medium, and high engagement groups.
- Ratings by Difficulty – Average rating per difficulty level.
- Cook Time Statistics – Spread and average of cook durations.
- Top Cooking Cities – Cities with the highest cook attempts.
- Weighted Popularity Score – Composite score using multiple interaction types.
- Steps by Difficulty – Harder recipes mapped to higher step counts.
- Engagement by Cuisine – Cuisines generating strongest non-view engagement.
- User State Distribution – Top states contributing to platform activity.

Outputs:
All results are exported as JSON summaries, CSV tables, and visual charts in Analytics_Output/.


# Pipeline Orchestration (run_pipeline.py)
The entire workflow is orchestrated through run_pipeline.py, which coordinates all stages—Firestore setup, ETL, validation, and analytics—into a single automated execution. The orchestration uses centralized logging from utils.py, ensuring consistent timestamps, event tracking, and operational transparency across all modules.

# How the Orchestration Works:
The run_pipeline.py script runs the pipeline in four sequential steps, each logged clearly:
# 1.Firestore Seeding
Populates Firestore with the primary (Idli Sambar) recipe, synthetic recipes, users, interactions, and intentionally injected bad data.
# 2.ETL (Extract & Transform)
Extracts Firestore collections, normalizes nested documents into relational tables, and writes structured CSVs into ETL_Output/.
# 3.Data Validation
Applies all quality rules, separates valid and invalid records into clean and quarantine folders, and produces a validation report.
# 4.Analytics Generation
Processes clean data to compute 15 insights, generate charts, and produce the final analytics report and summary tables.

# Role of utils.py
utils.py provides foundational utilities for the pipeline:
- Standardized logging for every module
- Dynamic directory creation for ETL, validation, and analytics folders
- Environment variable loading for project and service account configuration
- ID normalization helpers
- Timestamp/ISO utilities
- Benefits of This Orchestration
- Fully automated end-to-end pipeline
- Ensures a deterministic sequence—no stages are skipped or misordered
- Centralized logging improves traceability and debugging
- Modular structure simplifies maintenance and scaling
- Ideal for scheduling (Cron, Cloud Run, Airflow) in future expansions 

# How to Run the Complete Pipeline:
Follow the steps below to execute the entire workflow—from Firestore seeding to analytics generation.
# 1. Install dependencies
Install all required libraries listed in requirements.txt:
```
pip install -r requirements.txt
```

# 2. Configuration
Firebase configuration is already embedded inside utils.py:
```
PROJECT_ID = "assesment-amitkumarbande"
SERVICE_ACCOUNT_PATH = "D:\\Assignment_DataEngineer\\serviceAccountKey.json"
```
# 3. Run full pipeline:
Execute the orchestrator script:
```
python run_pipeline.py
```
This will automatically run all four stages in sequence:

# Step 1: Firestore Setup
Seeds Firestore with:
- Your primary recipe (Idli Sambar)
- 18 realistic synthetic recipes
- 30 users
- 400 interactions
- Injected bad data for validation testing

# Step 2: ETL (Extract + Transform)
Extracts Firestore collections and generates:
- recipe.csv
- ingredients.csv
- steps.csv
- users.csv
- interactions.csv

Stored under:  ETL_Output/

# Step 3: Data Validation
Validates all CSVs and splits them into:
  Validation_Output/
   clean_*.csv
   quarantine_*.csv
   validation_report.json

# Step 4: Analytics & Visualizations
Generates:
- analytics_report.json
- top_views.csv, user_engagement.csv, etc.
- 15 insight charts

Outputs stored in:
  Analytics_Output/
  Analytics_Output/Charts/

# Verify Output Folders
After the pipeline completes successfully, you will see:
  ETL_Output/
  Validation_Output/
  Analytics_Output/
  Analytics_Output/Charts/
Each folder contains structured, validated, and analytics-ready data.

# Known Limitations
- The pipeline performs a full refresh on every run; incremental loads are not implemented.
- Synthetic data and random interactions may not fully reflect real-world behavioral patterns.
- Firestore batch writes are limited to fixed-size batches (e.g., 400 records).
- Validation covers major logical rules but can be expanded with more advanced schema checks.
- Analytics outputs depend strictly on generated data; insights may vary across runs.
- The solution runs locally and does not include cloud deployment, scheduling, or CI/CD integration.
