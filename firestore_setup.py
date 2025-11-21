"""
firestore_setup.py
Purpose:
    Seeds Firebase Firestore with initial dataset required for the
    Recipe Analytics Data Pipeline. This includes:
        - Primary recipe (Idli Sambar)
        - Synthetic recipe dataset (18 recipes)
        - User dataset (30 users with location metadata)
        - User interactions (views, likes, cook attempts, ratings)
        - Injected bad data for validation testing

Modules Used:
    - firebase_admin for Firestore operations
    - random, datetime for synthetic data generation
    - utils.py for project configuration, logging, and ID normalization

Output:
    Populated Firestore collections:
        - recipes
        - users
        - interactions

Notes:
    This script is the first step of the pipeline orchestration.
    It is triggered automatically when running run_pipeline.py.
"""

import firebase_admin
from firebase_admin import credentials, firestore
import random
import datetime
from utils import (
    SERVICE_ACCOUNT_PATH, PROJECT_ID, get_logger, normalize_id
)

logger = get_logger("FirestoreSetup")

# config 
NUM_USERS = 30
NUM_INTERACTIONS = 400

#  location  
LOCATIONS = {
    "Maharashtra": ["Pune", "Mumbai", "Nagpur", "Nashik", "Thane"],
    "Karnataka": ["Bengaluru", "Mysuru", "Mangaluru", "Hubli"],
    "Tamil Nadu": ["Chennai", "Coimbatore", "Madurai", "Salem"],
    "Delhi": ["New Delhi"],
    "Telangana": ["Hyderabad", "Warangal"],
    "West Bengal": ["Kolkata", "Howrah"],
    "Gujarat": ["Ahmedabad", "Surat", "Vadodara"],
    "Rajasthan": ["Jaipur", "Udaipur"]
}

# user names
USER_NAMES = [
    "Amit Sharma", "Rohan Patil", "Sneha Verma", "Priya Joshi", "Kiran Shetty",
    "Aishwarya Iyer", "Rahul Deshmukh", "Vikas Rao", "Ritika Gupta", "Megha Pandey",
    "Suresh Chauhan", "Anita Mehta", "Deepak Mishra", "Anjali Khan", "Ravi Sharma",
    "Nikhil Patil", "Tanvi Verma", "Pooja Joshi", "Harsh Shetty", "Sanjay Iyer",
    "Varun Deshmukh", "Kavya Rao", "Sakshi Gupta", "Arjun Pandey", "Ishita Chauhan",
    "Manish Mehta", "Divya Mishra", "Karthik Khan", "Shreya Naik", "Neha Kulkarni"
]

# primary recipe (IDLI SAMBAR-own recipe) 
IDLI_SAMBAR = {
    "recipe_id": normalize_id("Idli Sambar"),
    "name": "Idli Sambar",
    "description": "Steamed idlis served with flavorful lentil sambar.",
    "prep_time_minutes": 20,
    "cook_time_minutes": 30,
    "servings": 4,
    "difficulty": "Medium",
    "cuisines": ["South Indian"],
    "tags": ["breakfast", "vegetarian", "steamed"],
    "ingredients": [
        {"ingredient_id": "idli_ing_1", "name": "Parboiled rice", "quantity": 2, "unit": "cups"},
        {"ingredient_id": "idli_ing_2", "name": "Urad dal", "quantity": 1, "unit": "cup"},
        {"ingredient_id": "idli_ing_3", "name": "Fenugreek seeds", "quantity": 1, "unit": "tsp"},
        {"ingredient_id": "idli_ing_4", "name": "Salt", "quantity": 1, "unit": "tsp"},
        {"ingredient_id": "idli_ing_5", "name": "Toor dal", "quantity": 0.5, "unit": "cup"},
        {"ingredient_id": "idli_ing_6", "name": "Sambar powder", "quantity": 2, "unit": "tbsp"},
    ],
    "steps": [
        {"step_no": 1, "instruction": "Soak rice and dal separately for 6 hours.", "duration_minutes": 0},
        {"step_no": 2, "instruction": "Grind to a smooth batter and ferment overnight.", "duration_minutes": 0},
        {"step_no": 3, "instruction": "Steam batter in idli molds for 10-12 mins.", "duration_minutes": 12},
        {"step_no": 4, "instruction": "Boil dal with veggies and spices for sambar.", "duration_minutes": 20},
    ],
}

# syntetic recipes
REALISTIC_RECIPES = [
    {
        "name": "Veg Fried Rice", "prep": 15, "cook": 10, "diff": "Easy", "tags": ["Chinese", "Lunch"],
        "ingredients": [
            ("Basmati Rice", 2, "cups"), ("Carrots", 1, "pc"), ("Beans", 0.5, "cup"), ("Soy Sauce", 1, "tbsp")
        ],
        "steps": ["Boil rice and cool it.", "Chop vegetables finely.", "Stir fry veggies on high heat.", "Add rice and sauces, toss well."]
    },
    {
        "name": "Paneer Butter Masala", "prep": 20, "cook": 30, "diff": "Medium", "tags": ["Dinner", "North Indian"],
        "ingredients": [
            ("Paneer", 250, "g"), ("Butter", 2, "tbsp"), ("Tomato Puree", 1, "cup"), ("Fresh Cream", 0.5, "cup")
        ],
        "steps": ["Fry paneer cubes lightly.", "Cook tomato puree with spices.", "Add cream and butter.", "Simmer paneer in gravy."]
    },
    {
        "name": "Spaghetti Aglio e Olio", "prep": 5, "cook": 15, "diff": "Easy", "tags": ["Italian", "Dinner"],
        "ingredients": [
            ("Spaghetti", 200, "g"), ("Garlic", 6, "cloves"), ("Olive Oil", 0.25, "cup"), ("Chili Flakes", 1, "tsp")
        ],
        "steps": ["Boil pasta until al dente.", "Sauté minced garlic in olive oil.", "Toss pasta in oil.", "Garnish with parsley."]
    },
    {
        "name": "Masoor Dal Tadka", "prep": 5, "cook": 20, "diff": "Easy", "tags": ["Comfort", "Indian"],
        "ingredients": [
            ("Masoor Dal", 1, "cup"), ("Onion", 1, "pc"), ("Tomato", 1, "pc"), ("Cumin Seeds", 1, "tsp")
        ],
        "steps": ["Wash and pressure cook dal.", "Prepare tadka with ghee, cumin, onion, tomato.", "Mix tadka into dal.", "Simmer for 5 mins."]
    },
    {
        "name": "Chana Masala", "prep": 480, "cook": 40, "diff": "Medium", "tags": ["Vegan", "North Indian"],
        "ingredients": [
            ("Chickpeas", 1.5, "cups"), ("Onion", 2, "pcs"), ("Chana Masala Powder", 2, "tbsp"), ("Ginger Garlic Paste", 1, "tbsp")
        ],
        "steps": ["Soak chickpeas overnight.", "Pressure cook until soft.", "Sauté onion paste and spices.", "Cook chickpeas in gravy."]
    },
    {
        "name": "Palak Paneer", "prep": 20, "cook": 25, "diff": "Medium", "tags": ["Healthy", "Dinner"],
        "ingredients": [
            ("Spinach", 2, "bunches"), ("Paneer", 200, "g"), ("Garlic", 4, "cloves"), ("Green Chili", 2, "pcs")
        ],
        "steps": ["Blanch spinach and puree it.", "Sauté garlic and spices.", "Add puree and cook.", "Add paneer cubes."]
    },
    {
        "name": "Chicken Tikka", "prep": 60, "cook": 25, "diff": "Medium", "tags": ["Appetizer", "Non-Veg"],
        "ingredients": [
            ("Chicken Breast", 500, "g"), ("Yogurt", 1, "cup"), ("Tikka Masala", 2, "tbsp"), ("Lemon Juice", 1, "tbsp")
        ],
        "steps": ["Marinate chicken with yogurt and spices for 1 hour.", "Skewer the pieces.", "Grill or bake at 200°C.", "Baste with butter."]
    },
    {
        "name": "Vegetable Biryani", "prep": 40, "cook": 45, "diff": "Hard", "tags": ["Main Course", "Rice"],
        "ingredients": [
            ("Basmati Rice", 2, "cups"), ("Mixed Veggies", 1.5, "cups"), ("Yogurt", 0.5, "cup"), ("Fried Onions", 0.5, "cup")
        ],
        "steps": ["Marinate veggies in yogurt.", "Par-boil rice with whole spices.", "Layer veggies and rice.", "Dum cook on low heat."]
    },
    {
        "name": "Rava Upma", "prep": 5, "cook": 15, "diff": "Easy", "tags": ["Breakfast", "South Indian"],
        "ingredients": [
            ("Rava (Semolina)", 1, "cup"), ("Mustard Seeds", 1, "tsp"), ("Curry Leaves", 5, "pcs"), ("Onion", 1, "pc")
        ],
        "steps": ["Roast rava until fragrant.", "Temper mustard seeds and curry leaves.", "Sauté onions and veggies.", "Add hot water and rava, stir well."]
    },
    {
        "name": "Aloo Gobi", "prep": 15, "cook": 25, "diff": "Medium", "tags": ["Lunch", "Dry Curry"],
        "ingredients": [
            ("Potatoes", 2, "pcs"), ("Cauliflower", 1, "head"), ("Turmeric", 0.5, "tsp"), ("Coriander Powder", 1, "tsp")
        ],
        "steps": ["Cut potatoes and cauliflower.", "Fry them until golden.", "Sauté ginger and spices.", "Toss veggies in masala."]
    },
    {
        "name": "Fish Curry", "prep": 20, "cook": 30, "diff": "Hard", "tags": ["Seafood", "Dinner"],
        "ingredients": [
            ("Fish Fillet", 500, "g"), ("Coconut Milk", 1.5, "cups"), ("Tamarind Paste", 1, "tbsp"), ("Curry Leaves", 6, "pcs")
        ],
        "steps": ["Marinate fish with turmeric.", "Make a curry base with coconut milk.", "Add tamarind and boil.", "Poach fish in the curry."]
    },
    {
        "name": "Egg Bhurji", "prep": 5, "cook": 10, "diff": "Easy", "tags": ["Breakfast", "Protein"],
        "ingredients": [
            ("Eggs", 4, "pcs"), ("Onion", 2, "pcs"), ("Green Chili", 2, "pcs"), ("Coriander", 1, "tbsp")
        ],
        "steps": ["Sauté chopped onions and chilies.", "Crack eggs into the pan.", "Scramble continuously.", "Garnish with coriander."]
    },
    {
        "name": "Lemon Rice", "prep": 10, "cook": 10, "diff": "Easy", "tags": ["South Indian", "Lunch"],
        "ingredients": [
            ("Cooked Rice", 3, "cups"), ("Lemon Juice", 2, "tbsp"), ("Peanuts", 2, "tbsp"), ("Turmeric", 0.5, "tsp")
        ],
        "steps": ["Heat oil and roast peanuts.", "Add mustard seeds and turmeric.", "Mix rice gently.", "Turn off heat and add lemon juice."]
    },
    {
        "name": "Mushroom Masala", "prep": 15, "cook": 25, "diff": "Medium", "tags": ["Dinner", "Vegetarian"],
        "ingredients": [
            ("Mushrooms", 200, "g"), ("Onion", 2, "pcs"), ("Tomato", 1, "pc"), ("Garam Masala", 1, "tsp")
        ],
        "steps": ["Sauté onion and tomato paste.", "Add spices and cook oil separates.", "Add sliced mushrooms.", "Cook covered until soft."]
    },
    {
        "name": "Rajma Chawal", "prep": 480, "cook": 40, "diff": "Medium", "tags": ["Comfort", "Lunch"],
        "ingredients": [
            ("Kidney Beans", 1.5, "cups"), ("Onion", 2, "pcs"), ("Tomato Puree", 1, "cup"), ("Rajma Masala", 2, "tbsp")
        ],
        "steps": ["Soak beans overnight.", "Pressure cook with salt.", "Prepare spicy onion-tomato gravy.", "Simmer beans in gravy."]
    },
    {
        "name": "Vegetable Pulao", "prep": 15, "cook": 20, "diff": "Easy", "tags": ["One Pot", "Rice"],
        "ingredients": [
            ("Basmati Rice", 1.5, "cups"), ("Peas", 0.5, "cup"), ("Carrots", 0.5, "cup"), ("Whole Spices", 1, "tbsp")
        ],
        "steps": ["Sauté whole spices in ghee.", "Add veggies and rice.", "Add water (1:2 ratio).", "Cover and cook until fluffy."]
    },
    {
        "name": "Tomato Soup", "prep": 10, "cook": 20, "diff": "Easy", "tags": ["Soup", "Healthy"],
        "ingredients": [
            ("Tomatoes", 6, "pcs"), ("Garlic", 4, "cloves"), ("Butter", 1, "tbsp"), ("Black Pepper", 0.5, "tsp")
        ],
        "steps": ["Roast tomatoes and garlic.", "Blend into a smooth puree.", "Strain into a pot.", "Simmer with butter and pepper."]
    },
    {
        "name": "Poha", "prep": 10, "cook": 10, "diff": "Easy", "tags": ["Breakfast", "Maharashtrian"],
        "ingredients": [
            ("Thick Poha", 2, "cups"), ("Onion", 1, "pc"), ("Mustard Seeds", 1, "tsp"), ("Peanuts", 2, "tbsp")
        ],
        "steps": ["Rinse poha in a colander.", "Sauté peanuts and mustard seeds.", "Add onions and turmeric.", "Mix poha and steam covered."]
    }
]

#   bad data injection for validation 
def inject_bad_data(db, user_ids, recipe_ids):
    logger.warning("⚠️  INJECTING INVALID DATA FOR VALIDATION TESTING...")
    
    # 1. bad recipe: negative prep time
    bad_recipe_1 = {
        "recipe_id": "bad_recipe_negative_time",
        "name": "Time Traveler Soup",
        "description": "Cooks before you start.",
        "prep_time_minutes": -10,
        "cook_time_minutes": 20,
        "difficulty": "Easy",
        "servings": 2,
        "ingredients": [],
        "steps": []
    }
    db.collection("recipes").document(bad_recipe_1["recipe_id"]).set(bad_recipe_1)

    # 2. bad recipe: invalid difficulty
    bad_recipe_2 = {
        "recipe_id": "bad_recipe_invalid_diff",
        "name": "Impossible Cake",
        "description": "Difficulty is not in allowed list.",
        "prep_time_minutes": 20,
        "cook_time_minutes": 20,
        "difficulty": "Expert", 
        "servings": 2,
        "ingredients": [],
        "steps": []
    }
    db.collection("recipes").document(bad_recipe_2["recipe_id"]).set(bad_recipe_2)

    # 3. bad recipe: missing name
    bad_recipe_3 = {
        "recipe_id": "bad_recipe_no_name",
        "name": "", 
        "description": "Has no name.",
        "prep_time_minutes": 10,
        "cook_time_minutes": 10,
        "difficulty": "Easy",
        "servings": 2,
        "ingredients": [],
        "steps": []
    }
    db.collection("recipes").document(bad_recipe_3["recipe_id"]).set(bad_recipe_3)

    # 4. bad interaction: rating out of bounds
    bad_inter_1 = {
        "interaction_id": "bad_int_rating_high",
        "user_id": random.choice(user_ids),
        "recipe_id": random.choice(recipe_ids),
        "type": "rating",
        "rating": 10, 
        "timestamp": datetime.datetime.utcnow()
    }
    db.collection("interactions").document(bad_inter_1["interaction_id"]).set(bad_inter_1)

    # 5. bad interaction: invalid type
    bad_inter_2 = {
        "interaction_id": "bad_int_wrong_type",
        "user_id": random.choice(user_ids),
        "recipe_id": random.choice(recipe_ids),
        "type": "shared_on_facebook", 
        "timestamp": datetime.datetime.utcnow()
    }
    db.collection("interactions").document(bad_inter_2["interaction_id"]).set(bad_inter_2)
    
    logger.info(" Injected 3 Bad Recipes & 2 Bad Interactions.")


def main():
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
            firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})
        
        db = firestore.client()
        
        # 1. add primary recipe
        logger.info("Adding Primary Recipe...")
        db.collection("recipes").document(IDLI_SAMBAR["recipe_id"]).set(IDLI_SAMBAR)
        
        # 2. Add Synthetic 
        logger.info("Adding 18 Synthetic Recipes with realistic steps...")
        for r in REALISTIC_RECIPES:
            rid = normalize_id(r["name"])
            
            # format ingredients for firestore
            firestore_ingredients = []
            for i, (name, qty, unit) in enumerate(r["ingredients"]):
                firestore_ingredients.append({
                    "ingredient_id": f"{rid}_ing_{i}",
                    "name": name,
                    "quantity": qty,
                    "unit": unit
                })
            
            # format steps for firestore
            firestore_steps = []
            for i, step_desc in enumerate(r["steps"]):
                firestore_steps.append({
                    "step_no": i + 1,
                    "instruction": step_desc,
                    "duration_minutes": max(5, r["cook"] // len(r["steps"])) # Approx duration
                })

            recipe_doc = {
                "recipe_id": rid,
                "name": r["name"],
                "description": f"Authentic {r['name']} recipe.",
                "prep_time_minutes": r["prep"],
                "cook_time_minutes": r["cook"],
                "servings": 2,
                "difficulty": r["diff"],
                "tags": r["tags"],
                "cuisines": ["Indian" if "Masala" in r["name"] or "Biryani" in r["name"] else "Global"],
                "ingredients": firestore_ingredients,
                "steps": firestore_steps
            }
            db.collection("recipes").document(rid).set(recipe_doc)
            
        # 3. add users
        logger.info("Adding 30 Users with realistic locations...")
        user_ids = []
        states = list(LOCATIONS.keys())
        for i, name in enumerate(USER_NAMES):
            uid = normalize_id(name)
            state = random.choice(states)
            city = random.choice(LOCATIONS[state])
            
            db.collection("users").document(uid).set({
                "user_id": uid,
                "name": name,
                "state": state, 
                "city": city,
                "country": "India",
                "email": f"user{i}@example.com"
            })
            user_ids.append(uid)
            
        # 4. interactions
        logger.info("Generating Interactions...")
        all_recipe_ids = [normalize_id(r["name"]) for r in REALISTIC_RECIPES] + [IDLI_SAMBAR["recipe_id"]]
        
        batch = db.batch()
        batch_count = 0
        for i in range(NUM_INTERACTIONS):
            t = random.choices(["view", "like", "cook_attempt", "rating"], weights=[0.6, 0.2, 0.1, 0.1])[0]
            int_id = f"int_{i:04d}"
            data = {
                "interaction_id": int_id,
                "user_id": random.choice(user_ids),
                "recipe_id": random.choice(all_recipe_ids),
                "type": t,
                "timestamp": datetime.datetime.utcnow(),
                "rating": random.randint(1, 5) if t == "rating" else None,
                "like": True if t == "like" else None
            }
            batch.set(db.collection("interactions").document(int_id), data)
            batch_count += 1
            if batch_count == 400:
                batch.commit()
                batch = db.batch()
                batch_count = 0
        if batch_count > 0: batch.commit()

        # 5. inject bad data
        inject_bad_data(db, user_ids, all_recipe_ids)

        logger.info("Firestore Setup Complete (Realistic Data + Bad Data Injection).")
        
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        raise

if __name__ == "__main__":
    main()