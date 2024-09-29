"""
This file outlines a function to request data from SQL database by ingredient:
- The function reads login details from a file if it exists,
prompts to input ingredients, searches for recipes that contain them
and collects data about the recipes, including other ingredients, sorting
results by amount of ingredients matched and amount of other ingredients.
It prompts the user to choose one and displays its ingredients and instructions.
"""
import sys
from sqlalchemy import create_engine
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option("display.max_rows", None)
pandas.set_option("display.max_colwidth", None)


def get_data():
    """
This function requests data from SQL database by ingredient: it reads login
details from a file if it exists, prompts to input ingredients, searches
for recipes that contain them and collects data about the recipes,
including other ingredients, sorting results by amount of ingredients
matched and amount of other ingredients.
It prompts the user to choose one and displays its ingredients and instructions.
    """
    try:
        with open("login.txt", "r", encoding="utf-8") as file:
            login = file.read().splitlines()
            username = login[0]
            password = login[1]
    except FileNotFoundError:
        username = input("Username: ")
        password = input("Password: ")
        write = input("Would you like to save your login details? (y/n): ")
        if write == "y":
            with open("login.txt", "w", encoding="utf-8") as file:
                file.write(username + "\n" + password)

    engine = create_engine(
        "mysql+pyodbc://"
        + username
        + ":"
        + password
        + "@localhost/Cooking?driver=MySQL"
    )

    conn = engine.connect()

    print("Type the ingredients you would like to use; Type 'STOP' to end the query")
    ingredient_request = ""
    ingredients = []
    while ingredient_request != "STOP":
        ingredient_request = input("Ingredient: ")
        if ingredient_request != "STOP":
            ingredients.append(ingredient_request)

    # Use string formatting to directly inject ingredients into the SQL query
    formatted_ingredients = ", ".join(f"'{ingredient}'" for ingredient in ingredients)

    # SQL query with dynamically inserted ingredients list
    query = f"""
        SELECT recipe_details.recipe_id, recipe_name, ingredients.ingredient, amount, unit_name, descriptions
        FROM recipe_details
        JOIN ingredients ON recipe_details.ingredient = ingredients.ID
        JOIN measurement_units ON recipe_details.measurement = measurement_units.unit_id
        JOIN recipes ON recipe_details.recipe_id = recipes.recipe_id
        WHERE recipe_details.recipe_id IN (
            SELECT recipe_id 
            FROM recipe_details
            JOIN ingredients ON recipe_details.ingredient = ingredients.ID
            WHERE ingredients.ingredient IN ({formatted_ingredients})
            GROUP BY recipe_id
            HAVING COUNT(DISTINCT ingredients.ingredient) >= {len(ingredients)}
        )
    """

    # Execute the query without placeholders
    recipe_df = pandas.read_sql(query, conn)

    # Rest of your code unchanged
    print("guac1")
    print(recipe_df[recipe_df["recipe_name"] == "Guacamole"])

    if len(recipe_df["recipe_name"].unique()) < 10:
        query = f"""
        SELECT main_query.recipe_id, recipe_name, ingredients.ingredient, amount, unit_name, descriptions
        FROM (
            SELECT recipe_details.recipe_id
            FROM recipe_details
            JOIN ingredients ON recipe_details.ingredient = ingredients.ID
            WHERE ingredients.ingredient IN ({formatted_ingredients})
            GROUP BY recipe_id
            ORDER BY COUNT(DISTINCT CASE WHEN ingredients.ingredient IN ({formatted_ingredients}) THEN ingredients.ingredient END) DESC
        ) AS main_query
        JOIN recipe_details ON main_query.recipe_id = recipe_details.recipe_id
        JOIN ingredients ON recipe_details.ingredient = ingredients.ID
        JOIN measurement_units ON recipe_details.measurement = measurement_units.unit_id
        JOIN recipes ON recipe_details.recipe_id = recipes.recipe_id
        LIMIT 1000
        """
        recipe_df1 = pandas.read_sql(query, conn)
        recipe_df = pandas.concat([recipe_df, recipe_df1], ignore_index=True)
        recipe_df = recipe_df.drop_duplicates()

    if len(recipe_df) == 0:
        print("No recipes found with the given ingredients")
        conn.close()
        return

    print(recipe_df.columns)
    print("guac2")
    print(recipe_df[recipe_df["recipe_name"] == "Guacamole"])
    sys.stdout.reconfigure(encoding="utf-8")

    ten_recipes = pandas.DataFrame(
        {"recipe_name": recipe_df["recipe_name"].unique()[:10]}
    )
    ten_recipes = ten_recipes.assign(number=range(1, len(ten_recipes) + 1))

    print("guac3")
    print(recipe_df[recipe_df["recipe_name"] == "Guacamole"])
    print("Choose the recipe you would like to make by typing its number")
    print(ten_recipes[["number", "recipe_name"]])
    num = int(input("Number: "))
    chosen_recipe_name = ten_recipes.loc[
        ten_recipes["number"] == num, "recipe_name"
    ].iloc[0]
    recipe_df = recipe_df[recipe_df["recipe_name"] == chosen_recipe_name]

    print(recipe_df["recipe_name"].unique())
    print(recipe_df[["ingredient", "amount", "unit_name"]])
    print(recipe_df["descriptions"].unique())

    conn.close()
    return recipe_df


get_data()
