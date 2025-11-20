# Firebase-Pipeline

#  Data Engineering Assesment (Firebase-Based Recipe Analytics Pipeline)  
A complete end-to-end **Data Engineering workflow** using **Firestore**, **Python**, **ETL**, **Analytics**, and **Visualization**.

#  Project Summary
This project implements an end-to-end data engineering pipeline:
- Data Modeling  
- Firestore Database Population  
- ETL Export & Transformation  
- Analytics & Insights  
- Visualization Generation  
- Comprehensive Documentation  

My own recipe — Idli Sambar — is added as the primary dataset.  
Synthetic recipes, User profiles, and 400+ interactions support analysis and modeling.

# Data Model

The data model includes three core Firestore collections:

# Recipes (`recipes`)
# Purpose:  
Stores complete recipe information including metadata, ingredients, steps, and classifications.
# Schema
| Field            | Type          | Description          |
|------------------|---------------|----------------------|
| recipe_id        | string        | Unique ID            |
| name             | string        | Recipe name          |
| description      | string        | Summary              |
| prep_time_minutes| integer       | Prep time            |
| cook_time_minutes| integer       | Cook time            |
| servings         | integer       | Total servings       |
| difficulty       | string        | Easy / Medium / Hard |
| cuisines         | array         | Cuisine categories   |
| tags             | array         | Labels               |
| ingredients      | array<object> | List of ingredients  |
| steps            | array<object> | Cooking instructions |

# Ingredient Structure
{
 "ingredient_id": "recipe_syn_001_ing_1",
 "name": "Paneer cubes",
 "quantity": 250,
 "unit": "grams"
}


# Step Structure

{
 "step_no": 1,
 "instruction": "Cook tomato puree with spices.",
 "duration_minutes": 10
}


# Primary Dataset: Idli Sambar Recipe

recipe_id: "idli_sambar_001"
name: "Idli Sambar"
description: "Steamed rice cakes with lentil sambar and coconut chutney."
difficulty: "Medium"
ingredients: 8 items
steps: 5 steps

# Users (`users`)
# Schema

| Field       | Type      |
|-------------|-----------|
| user_id     | string    |
| name        | string    |
| email       | string    |
| signup_date | timestamp |
| country     | string    |
| state       | string    |
| city        | string    |
| phone       | string    |

# Interactions (`interactions`)
Represents all engagement between users and recipes.
# Schema

| Field          | Type           | Description                         |
|----------------|----------------|-------------------------------------|
| interaction_id | string         | Event ID                            |
| user_id        | string         | FK → users                          |
| recipe_id      | string         | FK → recipes                        |
| type           | string         | view / like / cook_attempt / rating |
| like           | boolean / null | Only true for likes                 |
| rating         | integer / null | For rating events                   |
| timestamp      | timestamp      | Event time                          |

# ER Diagram (Text)

 Users (1) --------- (Many) Interactions (Many) --------- (1) Recipes


#  How to Run the Pipeline
# Step 1 — Populate Firestore
```bash
python firestore_setup.py
```
# Step 2 — Export + Transform Data
```bash
python etl_export_transform.py
```
Outputs saved to:
```
outputs/
```
# Step 3 — Run Analytics
```bash
python analytics.py
```
Outputs saved to:
```
analytics/
```
---
# ETL Workflow

# Extract
Reads raw collections from Firestore:
- recipes  
- users  
- interactions  

# Transform
- Normalizes recipes into separate CSVs  
- Flattens ingredients & steps  
- Cleans timestamps & nulls  
- Prepares analytics-ready datasets  

# Load
- Loads into pandas  
- Computes insights  
- Generates charts  
- Saves analytics report  

#  Insights Summary
The following insights are auto-generated:

1. Most common ingredients  
2. Average preparation time  
3. Difficulty distribution  
4. Correlation between prep time & likes  
5. Top viewed recipes  
6. Ingredients linked to high engagement  
7. Most liked recipes  
8. Highest rated recipes  
9. Engagement distribution  
10. Popularity index  

These are saved in:
```
analytics/analytics_report.json
```

# Visualization Outputs
Generated PNG charts:
- difficulty_distribution.png  
- common_ingredients.png  
- top_views.png  
- prep_vs_likes.png  
- high_engagement_ingredients.png  

All stored under:
```
analytics/
```
# Known Limitations

| Limitation              | Description                    |
|-------------------------|--------------------------------|
| Synthetic data          | Not real-world behaviour       |
| No Firestore emulator   | Direct Firebase access         |
| Batch ETL only          | No incremental/streaming       |
| No authentication       | Security not fully implemented |
| Custom popularity index | Weighted metric                |
| Sparse ratings          | Fewer rating events            |

# Conclusion
This project demonstrates a full data engineering pipeline:
- Data modeling  
- Firestore population  
- ETL extraction & normalization  
- Analytical insights  
- Visualization generation  
