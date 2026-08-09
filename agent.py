"""
Food Recipe Agent with RAG
Combines semantic search over recipes with an LLM agent for question answering.

Features:
- RAG: Search recipes using embeddings (pgvector)
- Agent: Answer culinary questions using Llama 3 Instruct
- Tools: Recipe search, details, insertion template
"""

import json
import logging
from typing import List, Dict, Any, Optional

import lakebase
from databricks.sdk import WorkspaceClient

logger = logging.getLogger("food-agent")

_w = WorkspaceClient()

# Llama 3 Instruct via Foundation Models API
LLM_ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"


class RecipeAgent:
    """
    Agent that combines RAG search with LLM reasoning.
    """
    
    def __init__(self):
        self.embedding_model = None
        self._load_embedding_model()
    
    def _load_embedding_model(self):
        """Load sentence transformer model for embeddings."""
        try:
            from sentence_transformers import SentenceTransformer
            self.embedding_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
            logger.info("Loaded embedding model")
        except ImportError:
            logger.error("sentence-transformers not installed")
            raise
    
    def search_recipes_rag(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """
        Semantic search over recipe embeddings using pgvector.
        Returns complete recipe details including ingredients and images.
        
        Args:
            query: Natural language query (e.g., "asian chicken dish")
            top_k: Number of results to return
        
        Returns:
            List of matching recipes with similarity scores and full details
        """
        # Generate query embedding
        query_embedding = self.embedding_model.encode(query, convert_to_numpy=True)
        query_vector = query_embedding.tolist()
        
        # Vector similarity search with full recipe details including country flag
        # Using exact JOIN structure from user's query
        # ORDER BY similarity DESC to get top 5 most similar recipes
        results = lakebase.run_query(
            """
            WITH ranked_recipes AS (
                SELECT DISTINCT ON (r.external_id_meal)
                    r.external_id_meal,
                    r.name_meal,
                    r.instructions,
                    r.ingredient_measure,
                    r.image_recipe,
                    r.video_recipe,
                    r.country,
                    r.region,
                    ca.name as category_name,
                    ca.imagem as category_image,
                    c.image as country_flag,
                    e.chunk_text,
                    1 - (e.embedding <=> %s::vector) AS similarity
                FROM recipe_embeddings e
                JOIN recipe r ON e.guid_recipe = r.id_guid_sk
                JOIN country c ON r.country = c.country
                JOIN category ca ON ca.id_guid_sk = r.guid_category
                ORDER BY r.external_id_meal, similarity DESC
            )
            SELECT * FROM ranked_recipes
            ORDER BY similarity DESC
            LIMIT %s
            """,
            (json.dumps(query_vector), top_k)
        )
        
        # Enrich with ingredients for each recipe
        enriched_results = []
        for recipe in results:
            # Parse ingredient_measure JSON if it exists
            # Expected format: {"1":{"ingredient":"...","measure":"..."}, "2":{...}, ...}
            ingredients = []
            if recipe.get('ingredient_measure'):
                try:
                    import json as json_lib
                    ing_data = recipe['ingredient_measure']
                    
                    # If it's a string, parse it
                    if isinstance(ing_data, str):
                        ing_data = json_lib.loads(ing_data)
                    
                    # Convert dict to list of {ingredient, measure}
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
                        # Already in correct format
                        for item in ing_data:
                            if isinstance(item, dict) and item.get('ingredient'):
                                ingredients.append(item)
                except Exception as e:
                    logger.error(f"Failed to parse ingredients: {e}")
                    pass
            
            recipe['ingredients'] = ingredients
            enriched_results.append(recipe)
        
        return enriched_results
    
    def get_recipe_details(self, recipe_id: int) -> Optional[Dict[str, Any]]:
        """
        Get full recipe details including ingredients.
        """
        recipe = lakebase.run_query(
            """
            SELECT 
                r.id, r.name, r.category, r.area,
                r.thumbnail_url, r.instructions,
                r.youtube_url, r.source_url
            FROM recipe r
            WHERE r.id = %s
            """,
            (recipe_id,)
        )
        
        if not recipe:
            return None
        
        # Get ingredients
        ingredients = lakebase.run_query(
            """
            SELECT ingredient, measure
            FROM ingredients
            WHERE recipe_id = %s
            ORDER BY ingredient
            """,
            (recipe_id,)
        )
        
        result = recipe[0]
        result['ingredients'] = ingredients
        return result
    
    def create_recipe_template(self) -> Dict[str, Any]:
        """
        Return a structured template for recipe insertion.
        """
        return {
            "name": "<Recipe Name>",
            "category": "<Category: e.g., Chicken, Seafood, Vegetarian, Dessert>",
            "area": "<Country/Cuisine: e.g., Italian, Mexican, Indian, Japanese>",
            "instructions": "<Step-by-step cooking instructions>",
            "ingredients": [
                {
                    "ingredient": "<Ingredient name>",
                    "measure": "<Quantity, e.g., 2 cups, 500g>"
                }
            ],
            "thumbnail_url": "<Optional image URL>",
            "youtube_url": "<Optional YouTube video URL>",
            "source_url": "<Optional source URL>",
            "tags": "<Optional comma-separated tags>"
        }
    
    def answer_with_llm(self, question: str, context: str = "") -> str:
        """
        Answer a question using Llama 3 Instruct.
        
        Args:
            question: User's question
            context: Optional context (e.g., retrieved recipes)
        
        Returns:
            LLM response
        """
        messages = [
            {
                "role": "system",
                "content": "You are a helpful culinary assistant. Answer questions about cooking, ingredients, and recipes. Be concise and informative."
            }
        ]
        
        if context:
            messages.append({
                "role": "system",
                "content": f"Here is relevant context:\n{context}"
            })
        
        messages.append({
            "role": "user",
            "content": question
        })
        
        try:
            response = _w.serving_endpoints.query(
                name=LLM_ENDPOINT,
                messages=messages,
                max_tokens=500,
                temperature=0.7
            )
            
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return f"Sorry, I couldn't process your question: {e}"
    
    def route_query(self, user_input: str) -> Dict[str, Any]:
        """
        Main routing logic: decide whether to use RAG, LLM, or template.
        
        Args:
            user_input: User's natural language input
        
        Returns:
            Response dict with type and data
        """
        user_lower = user_input.lower()
        
        # Recipe search patterns
        recipe_keywords = [
            "receita", "recipe", "prato", "dish", "comer", "eat",
            "quero", "want", "me de", "give me", "sugerir", "suggest"
        ]
        
        # Insert recipe patterns
        insert_keywords = [
            "inserir", "insert", "adicionar", "add", "criar", "create",
            "nova receita", "new recipe"
        ]
        
        # Check for recipe insertion
        if any(kw in user_lower for kw in insert_keywords):
            return {
                "type": "template",
                "data": self.create_recipe_template(),
                "message": "Here's a template to insert a new recipe. Fill in the details:"
            }
        
        # ALWAYS use RAG for any query (assume user wants recipes)
        logger.info(f"Using RAG search for: {user_input}")
        recipes = self.search_recipes_rag(user_input, top_k=5)
        logger.info(f"RAG returned {len(recipes)} recipes")
        
        if not recipes or len(recipes) == 0:
            logger.warning("No recipes found")
            return {
                "type": "no_results",
                "data": [],
                "message": "No recipes found. Try keywords like 'chicken', 'pasta', 'dessert', etc."
            }
        
        # Log first recipe for debugging
        if recipes:
            first = recipes[0]
            logger.info(f"Top recipe: {first.get('name_meal')} (similarity: {first.get('similarity', 0):.2f})")
            logger.info(f"Country: {first.get('country')}, Flag: {'Yes' if first.get('country_flag') else 'No'}")
            logger.info(f"Ingredients: {len(first.get('ingredients', []))} items")
        
        # Simple message
        message = f"Found {len(recipes)} recipes matching your search."
        
        return {
            "type": "recipe_search",
            "data": recipes,
            "message": message
        }


# Global agent instance
_agent = None

def get_agent() -> RecipeAgent:
    """Get or create the global agent instance."""
    global _agent
    if _agent is None:
        _agent = RecipeAgent()
    return _agent