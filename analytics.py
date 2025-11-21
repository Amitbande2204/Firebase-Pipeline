"""
analytics.py


Generates analytical insights, summary tables, and visual charts using
validated (clean) data. Computes 15 insights including ingredient trends,
rating patterns, engagement funnels, and popularity scoring.

Produces analytics_report.json, summary CSVs, and charts under
Analytics_Output/.
""" 

import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from typing import Tuple

# utils contains required constants and get_logger function
from utils import VALIDATION_DIR, ANALYTICS_DIR, CHART_DIR, get_logger

# config 
# ensure directories exist (redundant but safe)
os.makedirs(ANALYTICS_DIR, exist_ok=True)
os.makedirs(CHART_DIR, exist_ok=True)

ANALYTICS_FILE = "analytics_report.json"
# placeholder for assignment PDF path (matches developer instruction)
ASSIGNMENT_PDF_PATH = "D:/Assignment_DataEngineer/data_engineer_test.pdf" 
logger = get_logger("Analytics")

# non-interactive backend for server/headless environments
plt.switch_backend('Agg') 

#  utility 
def save_chart(fig: plt.Figure, filename: str):
    """Tighten and save a matplotlib Figure to the charts folder."""
    fig.tight_layout()
    fig.savefig(os.path.join(CHART_DIR, filename), dpi=150)
    plt.close(fig)

# load data 
# the return type now includes the steps DataFrame (s_df)
def load_clean_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Loads CLEAN data from validation output."""
    try:
        r_df = pd.read_csv(os.path.join(VALIDATION_DIR, "clean_recipe.csv"))
        i_df = pd.read_csv(os.path.join(VALIDATION_DIR, "clean_interactions.csv"))
        ing_df = pd.read_csv(os.path.join(VALIDATION_DIR, "clean_ingredients.csv"))
        u_df = pd.read_csv(os.path.join(VALIDATION_DIR, "clean_users.csv")) 
        s_df = pd.read_csv(os.path.join(VALIDATION_DIR, "clean_steps.csv")) # Load steps
        return r_df, i_df, ing_df, u_df, s_df
    except FileNotFoundError as e:
        logger.error(f"Clean data not found. Run validation.py first. Error: {e}")
        raise

#  insights 

#  most common ingredients
def insight_common_ingredients(ing_df):
    common = ing_df["name"].value_counts().head(5)
    fig, ax = plt.subplots(figsize=(8, 5))
    common.sort_values().plot(kind='barh', color='green', ax=ax)
    ax.set_title("1. Top 5 Most Common Ingredients")
    ax.set_xlabel("Count")
    ax.set_ylabel("Ingredient Name")
    save_chart(fig, "01_common_ingredients.png")
    return common.to_dict()

#  average preparation time
def insight_prep_time(r_df):
    avg_time = r_df["prep_time_minutes"].mean()
    fig, ax = plt.subplots(figsize=(6, 4))
    r_df["prep_time_minutes"].hist(bins=10, ax=ax, color='teal')
    ax.axvline(avg_time, color='red', linestyle='dashed', linewidth=1)
    ax.text(avg_time + 5, ax.get_ylim()[1]*0.8, f'Avg: {avg_time:.1f} min', color='red')
    ax.set_title("2. Recipe Preparation Time Distribution")
    ax.set_xlabel("Preparation Time (minutes)")
    ax.set_ylabel("Number of Recipes")
    save_chart(fig, "02_prep_time_dist.png")
    return {"average_prep_time_minutes": avg_time}

#  difficulty distribution
def insight_difficulty(r_df):
    difficulty_order = ["Easy", "Medium", "Hard"]
    dist = r_df["difficulty"].value_counts().reindex(difficulty_order, fill_value=0)
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.pie(dist, labels=dist.index, autopct='%1.1f%%', startangle=90, colors=['#4CAF50', '#FFC107', '#F44336'])
    ax.set_title("3. Recipe Difficulty Distribution")
    save_chart(fig, "03_difficulty_distribution.png")
    return dist.to_dict()

#  correlation between prep time and likes
def insight_prep_vs_likes(r_df, i_df):
    likes = i_df[i_df["type"] == "like"].groupby("recipe_id").size().reset_index(name="like_count")
    merged = r_df.merge(likes, on="recipe_id", how="left").fillna(0)
    correlation = merged["prep_time_minutes"].corr(merged["like_count"])
    
    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(merged["prep_time_minutes"], merged["like_count"], alpha=0.6, color='purple')
    ax.set_title(f"4. Prep Time vs. Likes (Correlation: {correlation:.2f})")
    ax.set_xlabel("Preparation Time (minutes)")
    ax.set_ylabel("Total Likes")
    save_chart(fig, "04_prep_vs_likes.png")
    return {"correlation_prep_time_likes": correlation}

# most frequently viewed recipes
def insight_top_views(r_df, i_df):
    views = i_df[i_df["type"] == "view"].groupby("recipe_id").size().reset_index(name="view_count")
    top_views = r_df.merge(views, on="recipe_id").sort_values("view_count", ascending=False).head(5)
    
    fig, ax = plt.subplots(figsize=(8, 5))
    top_views.sort_values("view_count").plot(kind='barh', x='name', y='view_count', legend=False, color='orange', ax=ax)
    ax.set_title("5. Top 5 Most Viewed Recipes")
    ax.set_xlabel("View Count")
    ax.set_ylabel("Recipe Name")
    save_chart(fig, "05_top_views.png")
    return top_views[["name", "view_count"]].to_dict(orient="records")

#  ingredients associated with high engagement
def insight_high_engagement_ingredients(r_df, i_df, ing_df):
    engagement = i_df[i_df["type"].isin(["like", "cook_attempt", "rating"])].groupby("recipe_id").size().reset_index(name="engagement_score")
    top_recipes = engagement.sort_values("engagement_score", ascending=False).head(5)["recipe_id"].tolist()
    
    top_ing = ing_df[ing_df["recipe_id"].isin(top_recipes)]["name"].value_counts().head(5)
    
    fig, ax = plt.subplots(figsize=(8, 5))
    top_ing.sort_values().plot(kind='barh', color='red', ax=ax)
    ax.set_title("6. Top Ingredients in High-Engagement Recipes")
    ax.set_xlabel("Ingredient Count")
    ax.set_ylabel("Ingredient Name")
    save_chart(fig, "06_high_engagement_ingredients.png")
    return top_ing.to_dict()

#  funnel (View -> Like -> Cook Attempt)
def insight_conversion_funnel(i_df):
    views = i_df[i_df["type"] == "view"].shape[0]
    likes = i_df[i_df["type"] == "like"].shape[0]
    cooks = i_df[i_df["type"] == "cook_attempt"].shape[0]
    
    data = [views, likes, cooks]
    labels = ["View", "Like", "Cook Attempt"]
    
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(labels, data, color=['#1f77b4', '#ff7f0e', '#2ca02c'])
    ax.set_title("7. User Interaction Funnel")
    ax.set_ylabel("Count")
    save_chart(fig, "07_interaction_funnel.png")
    
    return {"view_count": views, "like_count": likes, "cook_attempt_count": cooks}

#  user Segments (e.g., Low, Medium, High Engager)
def insight_user_segments(i_df):
    engagement = i_df.groupby("user_id").size().reset_index(name="total_interactions")
    
    bins = [0, engagement["total_interactions"].quantile(0.33), engagement["total_interactions"].quantile(0.66), engagement["total_interactions"].max() + 1]
    labels = ["Low Engager", "Medium Engager", "High Engager"]
    engagement["segment"] = pd.cut(engagement["total_interactions"], bins=bins, labels=labels, right=False)
    
    segments = engagement["segment"].value_counts().reindex(labels, fill_value=0)
    
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.pie(segments, labels=segments.index, autopct='%1.1f%%', startangle=90, colors=['#a6cee3', '#1f78b4', '#b2df8a'])
    ax.set_title("8. User Engagement Segments")
    save_chart(fig, "08_user_segments.png")
    return segments.to_dict()

#  average Rating by Difficulty
def insight_rating_by_difficulty(r_df, i_df):
    ratings = i_df[i_df["type"] == "rating"].dropna(subset=["rating"])
    merged = r_df.merge(ratings, on="recipe_id", how="inner")
    
    difficulty_order = ["Easy", "Medium", "Hard"]
    avg_rating = merged.groupby("difficulty")["rating"].mean().reindex(difficulty_order)
    
    fig, ax = plt.subplots(figsize=(8, 5))
    avg_rating.plot(kind='bar', color=['#4CAF50', '#FFC107', '#F44336'], ax=ax)
    ax.set_title("9. Average Rating by Recipe Difficulty")
    ax.set_ylabel("Average Rating (1-5)")
    ax.tick_params(axis='x', rotation=0)
    save_chart(fig, "09_rating_by_difficulty.png")
    
    return avg_rating.to_dict()

#  cook Time Statistics
def insight_cook_time(r_df):
    cook_stats = r_df["cook_time_minutes"].describe().to_dict()
    
    fig, ax = plt.subplots(figsize=(6, 4))
    r_df["cook_time_minutes"].hist(bins=10, ax=ax, color='sienna')
    ax.set_title("10. Recipe Cook Time Distribution")
    ax.set_xlabel("Cook Time (minutes)")
    ax.set_ylabel("Number of Recipes")
    save_chart(fig, "10_cook_time_dist.png")
    return cook_stats

#  top Cities for Cooking (Cook Attempts)
def insight_top_cities_cooking(i_df, u_df):
    cooks = i_df[i_df["type"] == "cook_attempt"]
    merged = cooks.merge(u_df, on="user_id", how="inner")
    top_cities = merged["city"].value_counts().head(5)
    
    fig, ax = plt.subplots(figsize=(8, 5))
    top_cities.sort_values().plot(kind='barh', color='indigo', ax=ax)
    ax.set_title("11. Top 5 Cities by Cook Attempts")
    ax.set_xlabel("Total Cook Attempts")
    ax.set_ylabel("City")
    save_chart(fig, "11_top_cities_cooking.png")
    return top_cities.to_dict()

#  weighted Popularity Score
def insight_popularity_score(r_df, i_df):
    # weights: view=1, like=5, cook=10, rating=2
    weights = {"view": 1, "like": 5, "cook_attempt": 10, "rating": 2}
    
    popularity = i_df.copy()
    popularity["score"] = popularity["type"].map(weights)
    popularity_sum = popularity.groupby("recipe_id")["score"].sum().reset_index(name="popularity_score")
    
    merged = r_df.merge(popularity_sum, on="recipe_id").sort_values("popularity_score", ascending=False).head(5)
    
    fig, ax = plt.subplots(figsize=(8, 5))
    merged.sort_values("popularity_score").plot(kind='barh', x='name', y='popularity_score', legend=False, color='goldenrod', ax=ax)
    ax.set_title("12. Top 5 Recipes by Weighted Popularity Score")
    ax.set_xlabel("Popularity Score")
    ax.set_ylabel("Recipe Name")
    save_chart(fig, "12_weighted_popularity.png")
    return merged[["name", "popularity_score"]].to_dict(orient="records")

# average steps by  difficulty
def insight_steps_by_difficulty(r_df, s_df): 
    """Insight: Do Harder recipes have more steps? (Uses steps DF: s_df)"""
    # group steps data by recipe_id and find the maximum step_no (total steps)
    steps_per_recipe = s_df.groupby("recipe_id")["step_no"].max().reset_index(name="total_steps")
    
    merged = r_df.merge(steps_per_recipe, on="recipe_id", how="inner")
    
    # ensure order is consistent
    difficulty_order = ["Easy", "Medium", "Hard"]
    avg_steps = merged.groupby("difficulty")["total_steps"].mean().reindex(difficulty_order)
    
    fig, ax = plt.subplots(figsize=(8, 5))
    avg_steps.plot(kind='bar', color='darkblue', ax=ax)
    ax.set_title("13. Average Steps by Difficulty")
    ax.set_ylabel("Average Total Steps")
    ax.tick_params(axis='x', rotation=0)
    save_chart(fig, "13_steps_by_difficulty.png")
    
    return avg_steps.to_dict()

#  engagement by Cuisine
def insight_engagement_by_cuisine(r_df, i_df):
    i_df_non_view = i_df[i_df["type"] != "view"]
    engagement = i_df_non_view.groupby("recipe_id").size().reset_index(name="engagement_count")
    merged = r_df.merge(engagement, on="recipe_id", how="inner")
    
    # explode cuisines and group
    merged_cuisines = merged.assign(cuisines=merged['cuisines'].str.split('|')).explode('cuisines')
    merged_cuisines['cuisines'] = merged_cuisines['cuisines'].str.strip()
    
    cuisine_engagement = merged_cuisines.groupby("cuisines")["engagement_count"].sum().sort_values(ascending=False).head(5)
    
    fig, ax = plt.subplots(figsize=(8, 5))
    cuisine_engagement.sort_values().plot(kind='barh', color='olivedrab', ax=ax)
    ax.set_title("14. Total Engagement by Cuisine")
    ax.set_xlabel("Total Non-View Engagement")
    ax.set_ylabel("Cuisine")
    save_chart(fig, "14_engagement_by_cuisine.png")
    return cuisine_engagement.to_dict()

#  user State Distribution (Top 5)
def insight_user_state_dist(u_df):
    state_dist = u_df["state"].value_counts().head(5)
    
    fig, ax = plt.subplots(figsize=(8, 5))
    state_dist.sort_values().plot(kind='barh', color='firebrick', ax=ax)
    ax.set_title("15. Top 5 User State Distribution")
    ax.set_xlabel("User Count")
    ax.set_ylabel("State")
    save_chart(fig, "15_user_state_distribution.png")
    return state_dist.to_dict()

#  main function
def main():
    logger.info("Loading Clean Data for Analytics...")
    # loading 5 dataframes: r_df, i_df, ing_df, u_df, s_df (recipes, interactions, ingredients, users, steps)
    try:
        r_df, i_df, ing_df, u_df, s_df = load_clean_data()
    except Exception as e:
        logger.error(f"Failed to load clean data. Aborting analytics. Error: {e}")
        return

    # data preparation for summary tables
    top_views = insight_top_views(r_df, i_df)
    
    # interaction summary
    interaction_counts = i_df.groupby(["recipe_id", "type"]).size().unstack(fill_value=0).reset_index()
    
    # user engagement summary (total interactions per user)
    user_engagement = i_df.groupby("user_id").size().reset_index(name="total_interactions")
    engagement_df = u_df.merge(user_engagement, on="user_id", how="left").fillna(0)
    
    # popularity summary (using Insight 12 logic for a CSV output)
    weights = {"view": 1, "like": 5, "cook_attempt": 10, "rating": 2}
    popularity = i_df.copy()
    popularity["score"] = popularity["type"].map(weights)
    popularity_sum = popularity.groupby("recipe_id")["score"].sum().reset_index(name="popularity_score")
    popularity_df = r_df.merge(popularity_sum, on="recipe_id", how="left").fillna({"popularity_score": 0})
    
    # insights generation
    logger.info("Generating 15 Insights & Visualizations...")
    
    insights = {}
    
    insights["1_common_ingredients"] = insight_common_ingredients(ing_df)
    insights["2_avg_prep_time"] = insight_prep_time(r_df)
    insights["3_difficulty_dist"] = insight_difficulty(r_df)
    insights["4_prep_likes_corr"] = insight_prep_vs_likes(r_df, i_df)
    insights["5_top_viewed"] = insight_top_views(r_df, i_df)
    insights["6_high_engagement_ing"] = insight_high_engagement_ingredients(r_df, i_df, ing_df)
    insights["7_funnel"] = insight_conversion_funnel(i_df)
    insights["8_segments"] = insight_user_segments(i_df)
    insights["9_rating_by_diff"] = insight_rating_by_difficulty(r_df, i_df)
    insights["10_cook_time_stats"] = insight_cook_time(r_df)
    insights["11_top_cities_cooking"] = insight_top_cities_cooking(i_df, u_df)
    insights["12_weighted_popularity"] = insight_popularity_score(r_df, i_df)
    insights["13_avg_steps_by_difficulty"] = insight_steps_by_difficulty(r_df, s_df) 
    insights["14_engagement_by_cuisine"] = insight_engagement_by_cuisine(r_df, i_df)
    insights["15_user_state_distribution"] = insight_user_state_dist(u_df)

    logger.info(f"Analytics Generation Complete. Report and charts in {ANALYTICS_DIR}")

    # prepare final report JSON structure
    report = {
        "export_date": pd.Timestamp.now().isoformat(),
        "total_recipes_clean": len(r_df),
        "total_users_clean": len(u_df),
        "total_interactions_clean": len(i_df),
        "top_views_summary": top_views, 
        "user_engagement_summary": engagement_df.to_dict(orient="records"),
        "popularity_summary": popularity_df[["recipe_id", "name", "popularity_score"]].sort_values("popularity_score", ascending=False).head(10).to_dict(orient="records"),
        "interaction_summary": interaction_counts.to_dict(orient="records"),
        "insights": insights,
        "assignment_pdf_path": ASSIGNMENT_PDF_PATH
    }

    # save JSON report
    with open(os.path.join(ANALYTICS_DIR, ANALYTICS_FILE), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    # save auxiliary CSVs
    top_views_df = pd.DataFrame(insights["5_top_viewed"])
    top_views_df.to_csv(os.path.join(ANALYTICS_DIR, "top_views.csv"), index=False)
    engagement_df.to_csv(os.path.join(ANALYTICS_DIR, "user_engagement.csv"), index=False)
    popularity_df.to_csv(os.path.join(ANALYTICS_DIR, "popularity_score.csv"), index=False)
    interaction_counts.to_csv(os.path.join(ANALYTICS_DIR, "interaction_summary.csv"), index=False)


if __name__ == "__main__":
    main()