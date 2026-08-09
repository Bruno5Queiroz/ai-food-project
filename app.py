"""
Capstone Project - Food Recipe Web Application
Flask app for browsing and searching food recipes stored in Lakebase.

Usage:
    python app.py
"""

import logging
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, request, render_template

import lakebase
from agent import get_agent

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("food-recipe-app")

# Get the base directory
BASE_DIR = Path(__file__).parent

# Create Flask app with explicit template folder
app = Flask(__name__, 
            template_folder=str(BASE_DIR / 'templates'))

_w = WorkspaceClient()

# SQL table creation scripts directory
SQL_DIR = BASE_DIR / "sql"


def ensure_category_table():
    """Create the category table in Lakebase if it doesn't exist."""
    logger.info("Creating category table...")
    sql_file = SQL_DIR / "01_category.sql"
    with open(sql_file) as f:
        sql = f.read()
    lakebase.run_write(sql)
    logger.info("Category table ready")


def ensure_ingredients_table():
    """Create the ingredients table in Lakebase if it doesn't exist."""
    logger.info("Creating ingredients table...")
    sql_file = SQL_DIR / "02_ingredients.sql"
    with open(sql_file) as f:
        sql = f.read()
    lakebase.run_write(sql)
    logger.info("Ingredients table ready")


def ensure_country_table():
    """Create the country table in Lakebase if it doesn't exist."""
    logger.info("Creating country table...")
    sql_file = SQL_DIR / "03_country.sql"
    with open(sql_file) as f:
        sql = f.read()
    lakebase.run_write(sql)
    logger.info("Country table ready")


def ensure_recipe_table():
    """Create the recipe table in Lakebase if it doesn't exist."""
    logger.info("Creating recipe table...")
    sql_file = SQL_DIR / "04_recipe.sql"
    with open(sql_file) as f:
        sql = f.read()
    lakebase.run_write(sql)
    logger.info("Recipe table ready")


def run_migration_allow_null_external_id():
    """Allow NULL for external_id_meal to support user-added recipes."""
    try:
        logger.info("Running migration: Allow NULL for external_id_meal...")
        migration_sql = "ALTER TABLE recipe ALTER COLUMN external_id_meal DROP NOT NULL;"
        lakebase.run_write(migration_sql)
        logger.info("Migration completed successfully")
    except Exception as e:
        # Migration may fail if already applied or column is already nullable
        logger.warning(f"Migration already applied or failed: {e}")


def ensure_recipe_embeddings_table():
    """
    Create the recipe_embeddings table in Lakebase if it doesn't exist.
    This table stores vector embeddings for semantic search over recipes.
    """
    logger.info("Creating recipe_embeddings table...")
    sql_file = SQL_DIR / "05_recipe_embeddings.sql"
    with open(sql_file) as f:
        sql = f.read()
    lakebase.run_write(sql)
    logger.info("Recipe embeddings table ready")


def setup_all_tables():
    """Create all PostgreSQL tables in Lakebase."""
    logger.info("Setting up all PostgreSQL tables in Lakebase...")
    
    try:
        ensure_category_table()
        ensure_ingredients_table()
        ensure_country_table()
        ensure_recipe_table()
        run_migration_allow_null_external_id()
        ensure_recipe_embeddings_table()
        
        logger.info("✅ All tables created successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to create tables: {e}")
        return False


# Flask Routes

@app.route("/healthz")
def healthz():
    """Health check endpoint."""
    return jsonify({"status": "ok"})


@app.errorhandler(Exception)
def handle_exception(err):
    """Ensure all unhandled errors return JSON."""
    logger.exception("Unhandled exception while processing request")
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/")
def index():
    """Main page - Food Recipe Browser."""
    return render_template("index.html")


@app.route("/chat")
def chat_page():
    """Chat page - AI-powered recipe search with RAG."""
    return render_template("chat.html")


@app.route("/api/recipes")
def list_recipes():
    """List all recipes with pagination."""
    limit = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    
    recipes = lakebase.run_query(
        """
        SELECT 
            r.external_id_meal as id,
            r.name_meal as name,
            c.name as category,
            r.country as area,
            r.image_recipe as thumbnail_url,
            r.instructions,
            r.video_recipe as youtube_url,
            co.image as country_flag
        FROM recipe r
        LEFT JOIN category c ON c.id_guid_sk = r.guid_category
        JOIN country co ON r.country = co.country
        ORDER BY r.name_meal
        LIMIT %s OFFSET %s
        """,
        (limit, offset)
    )
    
    return jsonify(recipes)


@app.route("/api/recipes/<int:recipe_id>")
def get_recipe(recipe_id):
    """Get detailed recipe information including ingredients."""
    recipe = lakebase.run_query(
        """
        SELECT 
            r.external_id_meal as id,
            r.name_meal as name,
            c.name as category,
            r.country as area,
            r.image_recipe as thumbnail_url,
            r.instructions,
            r.video_recipe as youtube_url,
            r.source_url,
            r.ingredient_measure,
            r.date_modified
        FROM recipe r
        LEFT JOIN category c ON c.id_guid_sk = r.guid_category
        WHERE r.external_id_meal = %s
        """,
        (recipe_id,)
    )
    
    if not recipe:
        return jsonify({"error": "Recipe not found"}), 404
    
    recipe_data = recipe[0]
    
    # Parse ingredients from ingredient_measure JSON field
    # Expected format: {"1":{"ingredient":"...","measure":"..."}, "2":{...}, ...}
    ingredients = []
    if recipe_data.get('ingredient_measure'):
        try:
            import json
            ing_data = recipe_data['ingredient_measure']
            
            if isinstance(ing_data, str):
                ing_data = json.loads(ing_data)
            
            if isinstance(ing_data, dict):
                # Sort by numeric key to preserve order
                sorted_keys = sorted(ing_data.keys(), key=lambda x: int(x) if x.isdigit() else 999)
                for key in sorted_keys:
                    value = ing_data[key]
                    if isinstance(value, dict) and value.get('ingredient'):
                        ingredients.append({
                            'ingredient': value.get('ingredient', '').strip(),
                            'measure': value.get('measure', '').strip()
                        })
            elif isinstance(ing_data, list):
                for item in ing_data:
                    if isinstance(item, dict) and item.get('ingredient'):
                        ingredients.append(item)
        except Exception as e:
            print(f"Error parsing ingredients: {e}")
            pass
    
    recipe_data['ingredients'] = ingredients
    
    return jsonify(recipe_data)


@app.route("/api/categories")
def list_categories():
    """List all available recipe categories."""
    categories = lakebase.run_query(
        "SELECT id_guid_sk as id, name FROM category ORDER BY name"
    )
    return jsonify(categories)


@app.route("/api/categories/distinct")
def list_distinct_categories():
    """List distinct category names - hardcoded 14 main categories."""
    logger.info("=== /api/categories/distinct called ===")
    # Return the 14 main categories as requested
    categories = [
        {"name": "Beef"},
        {"name": "Chicken"},
        {"name": "Dessert"},
        {"name": "Lamb"},
        {"name": "Miscellaneous"},
        {"name": "Pasta"},
        {"name": "Pork"},
        {"name": "Seafood"},
        {"name": "Side"},
        {"name": "Starter"},
        {"name": "Vegan"},
        {"name": "Vegetarian"},
        {"name": "Breakfast"},
        {"name": "Goat"}
    ]
    logger.info(f"Returning {len(categories)} categories")
    return jsonify(categories)


@app.route("/api/countries")
def list_countries():
    """List all available countries/areas."""
    countries = lakebase.run_query(
        "SELECT id, name FROM country ORDER BY name"
    )
    return jsonify(countries)


@app.route("/api/recipes/by-category/<category>")
def recipes_by_category(category):
    """Get recipes filtered by category with full details."""
    limit = int(request.args.get("limit", 5))
    
    recipes = lakebase.run_query(
        """
        SELECT 
            r.external_id_meal as id,
            r.name_meal as name,
            c.name as category,
            r.country as area,
            r.image_recipe as thumbnail_url,
            r.instructions,
            r.ingredient_measure,
            r.video_recipe as youtube_url,
            c.imagem as category_image,
            co.image as country_flag
        FROM recipe r
        LEFT JOIN category c ON c.id_guid_sk = r.guid_category
        JOIN country co ON r.country = co.country
        WHERE LOWER(c.name) = LOWER(%s)
        ORDER BY r.name_meal
        LIMIT %s
        """,
        (category, limit)
    )
    
    # Enrich with parsed ingredients
    for recipe in recipes:
        ingredients = []
        if recipe.get('ingredient_measure'):
            try:
                import json
                ing_data = recipe['ingredient_measure']
                
                if isinstance(ing_data, str):
                    ing_data = json.loads(ing_data)
                
                if isinstance(ing_data, dict):
                    for key, value in ing_data.items():
                        if key and value and key.strip() and value.strip():
                            ingredients.append({
                                'ingredient': key.strip(),
                                'measure': value.strip()
                            })
                elif isinstance(ing_data, list):
                    for item in ing_data:
                        if isinstance(item, dict) and item.get('ingredient'):
                            ingredients.append(item)
            except:
                pass
        recipe['ingredients'] = ingredients
    
    return jsonify(recipes)


@app.route("/api/recipes/by-country/<country>")
def recipes_by_country(country):
    """Get recipes filtered by country/area."""
    limit = int(request.args.get("limit", 50))
    
    recipes = lakebase.run_query(
        """
        SELECT 
            r.external_id_meal as id,
            r.name_meal as name,
            c.name as category,
            r.country as area,
            r.image_recipe as thumbnail_url,
            co.image as country_flag
        FROM recipe r
        LEFT JOIN category c ON c.id_guid_sk = r.guid_category
        JOIN country co ON r.country = co.country
        WHERE LOWER(r.country) = LOWER(%s)
        ORDER BY r.name_meal
        LIMIT %s
        """,
        (country, limit)
    )
    
    return jsonify(recipes)


@app.route("/api/recipes/search")
def search_recipes():
    """Search recipes by name or ingredient_measure field."""
    query = request.args.get("q", "").strip()
    limit = int(request.args.get("limit", 50))
    
    if not query:
        return jsonify({"error": "No search query provided"}), 400
    
    recipes = lakebase.run_query(
        """
        SELECT DISTINCT
            r.external_id_meal as id,
            r.name_meal as name,
            c.name as category,
            r.country as area,
            r.image_recipe as thumbnail_url
        FROM recipe r
        LEFT JOIN category c ON c.id_guid_sk = r.guid_category
        WHERE 
            LOWER(r.name_meal) LIKE LOWER(%s)
            OR LOWER(r.ingredient_measure) LIKE LOWER(%s)
            OR LOWER(r.instructions) LIKE LOWER(%s)
        ORDER BY r.name_meal
        LIMIT %s
        """,
        (f"%{query}%", f"%{query}%", f"%{query}%", limit)
    )
    
    return jsonify({"query": query, "results": recipes})


@app.route("/api/stats")
def get_stats():
    """Get database statistics."""
    stats = {}
    
    # Count recipes
    recipe_count = lakebase.run_query("SELECT COUNT(*) as count FROM recipe")
    stats['total_recipes'] = recipe_count[0]['count'] if recipe_count else 0
    
    # Count categories
    category_count = lakebase.run_query("SELECT COUNT(*) as count FROM category")
    stats['total_categories'] = category_count[0]['count'] if category_count else 0
    
    # Count countries
    country_count = lakebase.run_query("SELECT COUNT(*) as count FROM country")
    stats['total_countries'] = country_count[0]['count'] if country_count else 0
    
    return jsonify(stats)


@app.route("/api/intelligent-search", methods=["POST"])
def intelligent_search():
    """
    Intelligent search endpoint for index.html.
    Detects:
    - Ingredient questions ("what is pepper")
    - Country-based queries ("brazilian recipes")
    - Normal recipe search
    """
    data = request.get_json()
    query = data.get('query', '').strip().lower()
    
    if not query:
        return jsonify({"error": "No query provided"}), 400
    
    # Detect ingredient question
    ingredient_keywords = ['what is', 'tell me about', 'describe', 'definition of', 'info about']
    is_ingredient_question = any(kw in query for kw in ingredient_keywords)
    
    if is_ingredient_question:
        # Extract ingredient name (after "what is", "tell me about", etc.)
        ingredient_name = query
        for kw in ingredient_keywords:
            if kw in query:
                ingredient_name = query.split(kw)[-1].strip()
                break
        
        # Remove common words
        ingredient_name = ingredient_name.replace('the', '').replace('ingredient', '').strip()
        
        # Search in ingredients table
        results = lakebase.run_query(
            """
            SELECT ingredient_name, description
            FROM ingredients
            WHERE LOWER(ingredient_name) LIKE %s
            LIMIT 1
            """,
            (f"%{ingredient_name}%",)
        )
        
        if results:
            ing = results[0]
            return jsonify({
                "type": "ingredient_info",
                "data": {
                    "name": ing['ingredient_name'],
                    "description": ing['description']
                },
                "message": f"**{ing['ingredient_name']}**: {ing['description']}"
            })
        else:
            return jsonify({
                "type": "no_results",
                "message": f"Sorry, I couldn't find information about '{ingredient_name}'. Try searching for recipes instead!"
            })
    
    # Detect country-based query
    country_keywords = ['from', 'recipes', 'cuisine', 'food']
    # Common country names/adjectives
    country_patterns = {
        'brazilian': 'Brazilian',
        'brazil': 'Brazilian',
        'italian': 'Italian',
        'italy': 'Italian',
        'mexican': 'Mexican',
        'mexico': 'Mexican',
        'indian': 'Indian',
        'india': 'Indian',
        'french': 'French',
        'france': 'French',
        'chinese': 'Chinese',
        'china': 'Chinese',
        'japanese': 'Japanese',
        'japan': 'Japanese',
        'thai': 'Thai',
        'thailand': 'Thai',
        'american': 'American',
        'usa': 'American',
        'greek': 'Greek',
        'greece': 'Greek',
        'spanish': 'Spanish',
        'spain': 'Spanish',
    }
    
    detected_country = None
    for pattern, country_name in country_patterns.items():
        if pattern in query:
            detected_country = country_name
            break
    
    logger.info(f"Query: '{query}', Detected country: {detected_country}")
    
    if detected_country:
        # Get recipes from this country
        logger.info(f"Searching recipes for country: {detected_country}")
        recipes = lakebase.run_query(
            """
            SELECT 
                r.external_id_meal as id,
                r.name_meal as name,
                c.name as category,
                r.country as area,
                r.image_recipe as thumbnail_url,
                co.image as country_flag
            FROM recipe r
            LEFT JOIN category c ON c.id_guid_sk = r.guid_category
            JOIN country co ON r.country = co.country
            WHERE r.country = %s
            LIMIT 20
            """,
            (detected_country,)
        )
        
        if recipes:
            return jsonify({
                "type": "recipe_list",
                "data": recipes,
                "message": f"Found {len(recipes)} {detected_country} recipes!"
            })
        else:
            return jsonify({
                "type": "no_results",
                "message": f"Sorry, I couldn't find any {detected_country} recipes."
            })
    
    # Default: normal recipe search
    recipes = lakebase.run_query(
        """
        SELECT 
            r.external_id_meal as id,
            r.name_meal as name,
            c.name as category,
            r.country as area,
            r.image_recipe as thumbnail_url,
            co.image as country_flag
        FROM recipe r
        LEFT JOIN category c ON c.id_guid_sk = r.guid_category
        JOIN country co ON r.country = co.country
        WHERE LOWER(r.name_meal) LIKE %s
        LIMIT 20
        """,
        (f"%{query}%",)
    )
    
    if recipes:
        return jsonify({
            "type": "recipe_list",
            "data": recipes,
            "message": f"Found {len(recipes)} recipes matching '{query}'!"
        })
    else:
        return jsonify({
            "type": "no_results",
            "message": f"No recipes found for '{query}'. Try a different search!"
        })


@app.route("/api/recipes/add", methods=["POST"])
def add_recipe():
    """
    Intelligent recipe insertion endpoint.
    Automatically handles:
    - Category GUID lookup
    - Region lookup from country
    - ingredient_measure parsing from "Ingredient 1 - measure 1; Ingredient 2 - measure 2;"
    - Auto timestamps
    """
    data = request.get_json()
    
    try:
        # 1. Get category GUID from category name
        category_name = data.get('category', '').strip()
        if not category_name:
            return jsonify({"error": "Category is required"}), 400
        
        category_result = lakebase.run_query(
            "SELECT id_guid_sk FROM category WHERE LOWER(name) = LOWER(%s) LIMIT 1",
            (category_name,)
        )
        
        if not category_result:
            return jsonify({"error": f"Category '{category_name}' not found. Valid categories: Dessert, Miscellaneous, Beef, Starter, Side, Vegetarian, Vegan, Pasta, Seafood, Goat, Pork, Chicken, Lamb, Breakfast"}), 400
        
        guid_category = category_result[0]['id_guid_sk']
        
        # 2. Get region from country
        country_name = data.get('country', '').strip()
        if not country_name:
            return jsonify({"error": "Country is required"}), 400
        
        country_result = lakebase.run_query(
            "SELECT region, country FROM country WHERE LOWER(country) = LOWER(%s) LIMIT 1",
            (country_name,)
        )
        
        if not country_result:
            # List available countries to help user
            available_countries = lakebase.run_query(
                "SELECT DISTINCT country FROM country ORDER BY country LIMIT 20"
            )
            country_list = [c['country'] for c in available_countries] if available_countries else []
            return jsonify({
                "error": f"Country '{country_name}' not found in database.",
                "available_countries": country_list,
                "hint": "Try one of: " + ", ".join(country_list[:10])
            }), 400
        
        region = country_result[0]['region']
        country = country_result[0]['country']  # Use exact country name from DB
        
        # 3. Parse ingredient_measure from "Ingredient 1 - measure 1; Ingredient 2 - measure 2;"
        ingredient_input = data.get('ingredient_measure', '').strip()
        ingredient_measure_json = {}
        
        if ingredient_input:
            pairs = [p.strip() for p in ingredient_input.split(';') if p.strip()]
            for idx, pair in enumerate(pairs, start=1):
                if ' - ' in pair:
                    ingredient, measure = pair.split(' - ', 1)
                    ingredient_measure_json[str(idx)] = {
                        "ingredient": ingredient.strip(),
                        "measure": measure.strip()
                    }
        
        import json
        ingredient_measure_str = json.dumps(ingredient_measure_json)
        
        # 4. Prepare recipe data
        name_meal = data.get('name_meal', '').strip()
        if not name_meal:
            return jsonify({"error": "Recipe name (name_meal) is required"}), 400
        
        instructions = data.get('instructions', '').strip()
        image_recipe = data.get('image_recipe', '').strip()
        video_recipe = data.get('video_recipe', '').strip()
        source_url = data.get('source_url', '').strip()
        
        # 5. Generate UUID and timestamps
        import uuid
        from datetime import datetime
        
        id_guid_sk = str(uuid.uuid4())
        date_modified = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 6. Insert into recipe table
        lakebase.run_query(
            """
            INSERT INTO recipe (
                id_guid_sk, external_id_meal, name_meal, guid_category, country, region,
                instructions, ingredient_measure, image_recipe, video_recipe, source_url,
                date_modified, __source, __ingested_at, __updated_at
            ) VALUES (
                %s, NULL, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, 'input_user', NOW(), NOW()
            )
            """,
            (
                id_guid_sk, name_meal, guid_category, country, region,
                instructions, ingredient_measure_str, image_recipe, video_recipe, source_url,
                date_modified
            )
        )
        
        return jsonify({
            "success": True,
            "message": f"Recipe '{name_meal}' added successfully!",
            "recipe_id": id_guid_sk,
            "category": category_name,
            "country": country,
            "region": region
        })
        
    except Exception as e:
        logger.error(f"Failed to add recipe: {e}")
        return jsonify({"error": f"Failed to add recipe: {str(e)}"}), 500


@app.route("/api/chat", methods=["POST"])
def chat():
    """
    Agent endpoint: processes user queries with RAG.
    
    Body: {"message": "chicken recipe"}
    
    Returns:
        {
            "type": "recipe_search" | "no_results",
            "message": "Summary",
            "data": [...recipes...]
        }
    """
    logger.info("=== /api/chat called ===")
    body = request.json if request.is_json else {}
    user_message = body.get("message", "").strip()
    logger.info(f"User message: {user_message}")
    
    if not user_message:
        logger.warning("No message provided")
        return jsonify({"error": "No message provided"}), 400
    
    try:
        agent = get_agent()
        logger.info("Agent loaded successfully")
        response = agent.route_query(user_message)
        logger.info(f"Response type: {response.get('type')}, data count: {len(response.get('data', []))}")
        return jsonify(response)
    except Exception as e:
        logger.exception(f"Error in chat endpoint: {e}")
        return jsonify({"error": str(e)}), 500


# Initialize tables on startup
def init_app():
    """Initialize the database tables if needed."""
    try:
        logger.info("🚀 Starting Food Recipe Web Application")
        
        # Log template folder path
        template_folder = app.template_folder
        logger.info(f"Template folder: {template_folder}")
        logger.info(f"Template folder exists: {Path(template_folder).exists()}")
        
        if Path(template_folder).exists():
            templates = list(Path(template_folder).glob('*.html'))
            logger.info(f"Found {len(templates)} templates: {[t.name for t in templates]}")
        
        logger.info("Setting up database tables...")
        
        if setup_all_tables():
            logger.info("✅ Database tables ready")
        else:
            logger.warning("⚠️ Could not verify all tables, but app will continue")
    except Exception as e:
        logger.error(f"❌ Error during initialization: {e}")
        # Don't exit - tables may already exist


if __name__ == '__main__':
    # Initialize database
    init_app()
    
    # Start Flask server
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))
    
    logger.info(f"🍽️ Food Recipe App running on http://{host}:{port}")
    app.run(debug=False, host=host, port=port)
