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

Notes:
    This script is typically the FIRST step of the pipeline orchestration.
    It can be skipped in subsequent runs when data already exists, by using
    the orchestration flag in run_pipeline.py.
"""


import firebase_admin
from firebase_admin import credentials, firestore
import random
import datetime
from utils import (
    SERVICE_ACCOUNT_PATH, PROJECT_ID, get_logger, normalize_id
)

logger = get_logger("FirestoreSetup")

NUM_USERS = 30
NUM_INTERACTIONS = 400


# User Locations

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

USER_NAMES = [
    "Amit Sharma", "Rohan Patil", "Sneha Verma", "Priya Joshi", "Kiran Shetty",
    "Aishwarya Iyer", "Rahul Deshmukh", "Vikas Rao", "Ritika Gupta", "Megha Pandey",
    "Suresh Chauhan", "Anita Mehta", "Deepak Mishra", "Anjali Khan", "Ravi Sharma",
    "Nikhil Patil", "Tanvi Verma", "Pooja Joshi", "Harsh Shetty", "Sanjay Iyer",
    "Varun Deshmukh", "Kavya Rao", "Sakshi Gupta", "Arjun Pandey", "Ishita Chauhan",
    "Manish Mehta", "Divya Mishra", "Karthik Khan", "Shreya Naik", "Neha Kulkarni"
]


def now_utc():
    return datetime.datetime.utcnow()


# --------------------------------------------------------
# PRIMARY RECIPE — IDLI SAMBAR (Final Optimized Version)
# --------------------------------------------------------
IDLI_SAMBAR = {
    "recipe_id": normalize_id("Idli Sambar"),
    "name": "Idli Sambar",
    "description": "Traditional South Indian breakfast of steamed idlis served with aromatic sambar.",
    "prep_time_minutes": 20,
    "cook_time_minutes": 45,
    "servings": 4,
    "difficulty": "Medium",
    "cuisines": ["South Indian"],
    "tags": ["breakfast", "vegetarian", "steamed", "traditional"],

    "ingredients": [
        {"ingredient_id": "idli_1", "name": "Idli rice", "quantity": 1.5, "unit": "cups"},
        {"ingredient_id": "idli_2", "name": "Urad dal", "quantity": 0.5, "unit": "cups"},
        {"ingredient_id": "idli_3", "name": "Fenugreek seeds", "quantity": 0.5, "unit": "tsp"},
        {"ingredient_id": "idli_4", "name": "Water", "quantity": 0, "unit": "as needed"},
        {"ingredient_id": "idli_5", "name": "Salt", "quantity": 1, "unit": "tsp"},
        {"ingredient_id": "idli_6", "name": "Oil (for greasing plates)", "quantity": 1, "unit": "tsp"},

        {"ingredient_id": "sambar_1", "name": "Toor dal", "quantity": 0.5, "unit": "cups"},
        {"ingredient_id": "sambar_2", "name": "Water", "quantity": 2, "unit": "cups"},
        {"ingredient_id": "sambar_3", "name": "Turmeric powder", "quantity": 0.25, "unit": "tsp"},
        {"ingredient_id": "sambar_4", "name": "Salt", "quantity": 0, "unit": "as per taste"},

        {"ingredient_id": "sambar_5", "name": "Onion (sliced)", "quantity": 1, "unit": "small"},
        {"ingredient_id": "sambar_6", "name": "Tomato (chopped)", "quantity": 1, "unit": "medium"},
        {"ingredient_id": "sambar_7", "name": "Carrot (diced)", "quantity": 0.5, "unit": "cup"},
        {"ingredient_id": "sambar_8", "name": "Drumstick pieces", "quantity": 5, "unit": "pcs"},
        {"ingredient_id": "sambar_9", "name": "Green chilli (slit)", "quantity": 1, "unit": "pc"},

        {"ingredient_id": "sambar_10", "name": "Sambar powder", "quantity": 1.5, "unit": "tbsp"},
        {"ingredient_id": "sambar_11", "name": "Tamarind", "quantity": 1, "unit": "lemon-sized"},
        {"ingredient_id": "sambar_12", "name": "Warm water", "quantity": 0.5, "unit": "cup"},
        {"ingredient_id": "sambar_13", "name": "Jaggery (optional)", "quantity": 0.5, "unit": "tsp"},

        {"ingredient_id": "tadka_1", "name": "Oil", "quantity": 1, "unit": "tbsp"},
        {"ingredient_id": "tadka_2", "name": "Mustard seeds", "quantity": 0.5, "unit": "tsp"},
        {"ingredient_id": "tadka_3", "name": "Cumin seeds", "quantity": 0.25, "unit": "tsp"},
        {"ingredient_id": "tadka_4", "name": "Curry leaves", "quantity": 10, "unit": "leaves"},
        {"ingredient_id": "tadka_5", "name": "Dry red chilli", "quantity": 1, "unit": "pc"},
        {"ingredient_id": "tadka_6", "name": "Hing", "quantity": 0.1, "unit": "tsp"},
    ],

    "steps": [
        {"step_no": 1, "instruction": "Wash idli rice 2–3 times and soak for 4–5 hours.", "duration_minutes": 5},
        {"step_no": 2, "instruction": "Wash urad dal + fenugreek seeds and soak for 4 hours.", "duration_minutes": 5},
        {"step_no": 3, "instruction": "Grind soaked urad dal with little water until fluffy.", "duration_minutes": 10},
        {"step_no": 4, "instruction": "Grind soaked rice to slightly coarse paste.", "duration_minutes": 10},
        {"step_no": 5, "instruction": "Mix both pastes with salt until combined.", "duration_minutes": 5},
        {"step_no": 6, "instruction": "Ferment batter 8–12 hours.", "duration_minutes": 480},

        {"step_no": 7, "instruction": "Heat idli steamer with water.", "duration_minutes": 2},
        {"step_no": 8, "instruction": "Grease idli plates lightly with oil.", "duration_minutes": 1},
        {"step_no": 9, "instruction": "Pour batter ¾th into moulds.", "duration_minutes": 2},
        {"step_no": 10, "instruction": "Steam idlis 10–12 minutes.", "duration_minutes": 12},
        {"step_no": 11, "instruction": "Cool and remove idlis.", "duration_minutes": 3},

        {"step_no": 12, "instruction": "Add toor dal, 2 cups water, turmeric to pressure cooker.", "duration_minutes": 2},
        {"step_no": 13, "instruction": "Pressure cook 3–4 whistles.", "duration_minutes": 15},
        {"step_no": 14, "instruction": "Mash cooked dal.", "duration_minutes": 2},

        {"step_no": 15, "instruction": "Boil onion, tomato, carrot, drumstick, chilli.", "duration_minutes": 15},
        {"step_no": 16, "instruction": "Add salt and soften vegetables.", "duration_minutes": 15},

        {"step_no": 17, "instruction": "Add mashed dal to vegetables.", "duration_minutes": 1},
        {"step_no": 18, "instruction": "Add sambar powder, tamarind water, jaggery.", "duration_minutes": 2},
        {"step_no": 19, "instruction": "Simmer 10 minutes on low flame.", "duration_minutes": 10},

        {"step_no": 20, "instruction": "Heat oil; add mustard seeds to crackle.", "duration_minutes": 2},
        {"step_no": 21, "instruction": "Add cumin, curry leaves, red chilli, hing.", "duration_minutes": 1},
        {"step_no": 22, "instruction": "Pour tempering into sambar.", "duration_minutes": 1},
        {"step_no": 23, "instruction": "Boil sambar 1–2 minutes.", "duration_minutes": 2},
    ],
}

# --------------------------------------------------------
# SYNTHETIC RECIPES (Full List)
# --------------------------------------------------------
REALISTIC_RECIPES = [
    {
        "name": "Veg Fried Rice",
        "prep": 15,
        "cook": 10,
        "diff": "Easy",
        "tags": ["Chinese", "Lunch"],
        "ingredients": [
            ("Basmati Rice", 2, "cups"),
            ("Carrots", 1, "pc"),
            ("Beans", 0.5, "cup"),
            ("Soy Sauce", 1, "tbsp"),
            ("Spring Onion", 2, "tbsp"),
        ],
        "steps": [
            "Boil rice and cool completely",
            "Chop vegetables finely",
            "Stir fry vegetables on high flame",
            "Add rice, soy sauce, mix well",
        ]
    },
    {
        "name": "Paneer Butter Masala",
        "prep": 20,
        "cook": 25,
        "diff": "Medium",
        "tags": ["Indian", "Dinner"],
        "ingredients": [
            ("Paneer", 200, "g"),
            ("Tomatoes", 3, "pcs"),
            ("Butter", 2, "tbsp"),
            ("Cream", 3, "tbsp"),
        ],
        "steps": [
            "Blend tomatoes to puree",
            "Cook puree with spices",
            "Add paneer cubes",
            "Finish with cream and butter",
        ]
    },
    {
        "name": "Aloo Paratha",
        "prep": 25,
        "cook": 15,
        "diff": "Medium",
        "tags": ["Indian", "Breakfast"],
        "ingredients": [
            ("Wheat Flour", 2, "cups"),
            ("Potatoes", 3, "pcs"),
            ("Ghee", 1, "tbsp"),
        ],
        "steps": [
            "Prepare dough",
            "Boil and mash potatoes",
            "Stuff parathas",
            "Cook on tawa with ghee",
        ]
    },
    {
        "name": "Masala Dosa",
        "prep": 30,
        "cook": 20,
        "diff": "Medium",
        "tags": ["South Indian", "Breakfast"],
        "ingredients": [
            ("Dosa Batter", 2, "cups"),
            ("Potatoes", 2, "pcs"),
            ("Curry Leaves", 10, "leaves"),
        ],
        "steps": [
            "Prepare masala stuffing",
            "Spread dosa on tawa",
            "Add stuffing",
            "Fold and roast",
        ]
    },
    {
        "name": "Chicken Biryani",
        "prep": 40,
        "cook": 45,
        "diff": "Hard",
        "tags": ["Indian"],
        "ingredients": [
            ("Chicken", 500, "g"),
            ("Basmati Rice", 2, "cups"),
            ("Curd", 1, "cup"),
        ],
        "steps": [
            "Marinate chicken",
            "Parboil rice",
            "Layer rice and chicken",
            "Cook on dum",
        ]
    },
    {
        "name": "Pav Bhaji",
        "prep": 20,
        "cook": 25,
        "diff": "Easy",
        "tags": ["Street Food"],
        "ingredients": [
            ("Pav Bhaji Masala", 2, "tsp"),
            ("Butter", 3, "tbsp"),
            ("Vegetables Mix", 2, "cups"),
        ],
        "steps": [
            "Boil vegetables",
            "Mash well",
            "Cook with masala + butter",
        ]
    },
    {
        "name": "Poha",
        "prep": 10,
        "cook": 5,
        "diff": "Easy",
        "tags": ["Breakfast"],
        "ingredients": [
            ("Poha", 2, "cups"),
            ("Onion", 1, "pc"),
            ("Lemon", 1, "pc"),
        ],
        "steps": [
            "Rinse poha",
            "Cook onion + spices",
            "Add poha",
        ]
    },
    {
        "name": "Dal Tadka",
        "prep": 15,
        "cook": 20,
        "diff": "Easy",
        "tags": ["Indian"],
        "ingredients": [
            ("Toor Dal", 1, "cup"),
            ("Ghee", 1, "tbsp"),
        ],
        "steps": [
            "Boil dal",
            "Prepare tadka",
            "Mix and simmer",
        ]
    },
    {
        "name": "Gulab Jamun",
        "prep": 10,
        "cook": 20,
        "diff": "Medium",
        "tags": ["Dessert"],
        "ingredients": [
            ("Milk Powder", 1, "cup"),
            ("Sugar", 1, "cup"),
        ],
        "steps": [
            "Prepare dough",
            "Fry balls",
            "Dip into sugar syrup",
        ]
    },
    {
        "name": "Mango Shake",
        "prep": 5,
        "cook": 0,
        "diff": "Easy",
        "tags": ["Drink"],
        "ingredients": [
            ("Mango", 1, "pc"),
            ("Milk", 1, "cup"),
        ],
        "steps": [
            "Blend all ingredients",
            "Serve chilled",
        ]
    },
    {
        "name": "Cutlet",
        "prep": 20,
        "cook": 10,
        "diff": "Medium",
        "tags": ["Snack"],
        "ingredients": [
            ("Breadcrumbs", 1, "cup"),
            ("Potatoes", 2, "pcs"),
        ],
        "steps": [
            "Make mixture",
            "Shape cutlets",
            "Shallow fry",
        ]
    },
    {
        "name": "Upma",
        "prep": 10,
        "cook": 10,
        "diff": "Easy",
        "tags": ["Breakfast"],
        "ingredients": [
            ("Rava", 1, "cup"),
            ("Vegetables", 1, "cup"),
        ],
        "steps": [
            "Roast rava",
            "Cook vegetables",
            "Mix with water + spices",
        ]
    },
    {
        "name": "Idiyappam",
        "prep": 20,
        "cook": 10,
        "diff": "Medium",
        "tags": ["Breakfast"],
        "ingredients": [
            ("Rice Flour", 1, "cup"),
            ("Coconut", 2, "tbsp"),
        ],
        "steps": [
            "Prepare dough",
            "Press into noodles",
            "Steam",
        ]
    },
    {
        "name": "Fish Fry",
        "prep": 15,
        "cook": 10,
        "diff": "Easy",
        "tags": ["Seafood"],
        "ingredients": [
            ("Fish", 4, "pcs"),
            ("Masala", 2, "tbsp"),
        ],
        "steps": [
            "Marinate fish",
            "Shallow fry",
        ]
    },
    {
        "name": "Khichdi",
        "prep": 15,
        "cook": 20,
        "diff": "Easy",
        "tags": ["Comfort Food"],
        "ingredients": [
            ("Rice", 1, "cup"),
            ("Dal", 0.5, "cup"),
        ],
        "steps": [
            "Pressure cook all ingredients",
            "Add ghee & serve",
        ]
    },
]

# --------------------------------------------------------
# BAD DATA INJECTION FOR TESTING
# --------------------------------------------------------
def inject_bad_data(db, user_ids, recipe_ids):
    logger.warning("Injecting invalid test data...")

    ts = now_utc()

    bad_recipe = {
        "recipe_id": "recipe_negative_prep",
        "name": "Impossible Dish",
        "description": "Invalid negative prep time",
        "prep_time_minutes": -10,
        "cook_time_minutes": 20,
        "difficulty": "Easy",
        "servings": 2,
        "ingredients": [],
        "steps": [],
        "created_at": ts,
        "updated_at": ts
    }

    bad_inter = {
        "interaction_id": "invalid_rating_high",
        "user_id": random.choice(user_ids),
        "recipe_id": random.choice(recipe_ids),
        "type": "rating",
        "rating": 10,  # invalid
        "timestamp": ts,
        "created_at": ts,
        "updated_at": ts
    }

    db.collection("recipes").document(bad_recipe["recipe_id"]).set(bad_recipe)
    db.collection("interactions").document(bad_inter["interaction_id"]).set(bad_inter)


# --------------------------------------------------------
# MAIN SEEDING PROCESS
# --------------------------------------------------------
def main():
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
            firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})

        db = firestore.client()
        ts = now_utc()

        # ---------------- Primary Recipe ----------------
        logger.info("Adding Primary Recipe: Idli Sambar")
        recipe_doc = IDLI_SAMBAR.copy()
        recipe_doc["created_at"] = ts
        recipe_doc["updated_at"] = ts
        db.collection("recipes").document(recipe_doc["recipe_id"]).set(recipe_doc)

        # ---------------- Synthetic Recipes -------------
        logger.info("Adding Synthetic Recipes...")
        synthetic_ids = []

        for rec in REALISTIC_RECIPES:
            rid = normalize_id(rec["name"])
            synthetic_ids.append(rid)

            ingredients = [
                {
                    "ingredient_id": f"{rid}_ing_{i}",
                    "name": ing[0],
                    "quantity": ing[1],
                    "unit": ing[2]
                }
                for i, ing in enumerate(rec["ingredients"])
            ]

            steps = [
                {
                    "step_no": i + 1,
                    "instruction": step,
                    "duration_minutes": max(5, rec["cook"] // len(rec["steps"]))
                }
                for i, step in enumerate(rec["steps"])
            ]

            db.collection("recipes").document(rid).set({
                "recipe_id": rid,
                "name": rec["name"],
                "description": f"{rec['name']} recipe",
                "prep_time_minutes": rec["prep"],
                "cook_time_minutes": rec["cook"],
                "difficulty": rec["diff"],
                "servings": 2,
                "tags": rec["tags"],
                "cuisines": ["Global"],
                "ingredients": ingredients,
                "steps": steps,
                "created_at": ts,
                "updated_at": ts
            })

        # ---------------- Users -------------------------
        logger.info("Adding Users...")
        user_ids = []

        for i, name in enumerate(USER_NAMES):
            uid = normalize_id(name)
            user_ids.append(uid)

            state = random.choice(list(LOCATIONS.keys()))
            city = random.choice(LOCATIONS[state])

            db.collection("users").document(uid).set({
                "user_id": uid,
                "name": name,
                "city": city,
                "state": state,
                "country": "India",
                "email": f"user{i}@example.com",
                "created_at": ts,
                "updated_at": ts
            })

        # ---------------- Interactions -------------------
        logger.info("Adding Interactions...")
        all_recipes = synthetic_ids + [recipe_doc["recipe_id"]]

        batch = db.batch()
        bcount = 0

        for i in range(NUM_INTERACTIONS):
            t = random.choice(["view", "like", "cook_attempt", "rating"])
            rec_id = random.choice(all_recipes)
            uid = random.choice(user_ids)

            inter = {
                "interaction_id": f"int_{i:04d}",
                "user_id": uid,
                "recipe_id": rec_id,
                "type": t,
                "rating": random.randint(1, 5) if t == "rating" else None,
                "like": True if t == "like" else None,
                "timestamp": ts,
                "created_at": ts,
                "updated_at": ts,
            }

            batch.set(
                db.collection("interactions").document(inter["interaction_id"]),
                inter
            )
            bcount += 1

            if bcount == 400:
                batch.commit()
                batch = db.batch()
                bcount = 0

        if bcount > 0:
            batch.commit()

        # ---------------- Bad Data -----------------------
        inject_bad_data(db, user_ids, all_recipes)

        logger.info("Firestore setup complete!")

    except Exception as e:
        logger.error(f"Firestore setup failed: {e}")
        raise


if __name__ == "__main__":
    main()
