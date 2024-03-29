import pandas
from sqlalchemy import create_engine
import sys

pandas.set_option("display.max_columns", None)
pandas.set_option("display.max_rows", None)
pandas.set_option("display.max_colwidth", None)


def get_data():
    try:
        with open("login.txt", "r") as file:
            login: list = file.read().splitlines()
            username: str = login[0]
            password: str = login[1]
    except FileNotFoundError:
        username: str = input("Username: ")
        password: str = input("Password: ")
        write: str = input("Would you like to save your login details? (y/n): ")
        if write == "y":
            with open("login.txt", "w") as file:
                file.write(username + "\n" + password)
    engine: create_engine = create_engine(
        "mysql+pyodbc://"
        + username
        + ":"
        + password
        + "@localhost/Cooking?driver=MySQL"
    )

    conn = engine.connect()
    print("Type the ingredients would like to use; Type 'STOP' to end the query")
    ingredient_request = ""
    ingredients = []
    while ingredient_request != "STOP":
        ingredient_request = input("Ingredient: ")
        ingredients.append(ingredient_request)
    ingredients.pop()

    query: str = """
        SELECT recipe_details.recipe_id, recipe_name, ingredient_name, amount, unit_name, recipe_descriptions
        FROM recipe_details
        JOIN ingredients ON recipe_details.ingredient = ingredients.ingredient_id
        JOIN measurement_units ON recipe_details.measurement = measurement_units.unit_id
        JOIN recipes ON recipe_details.recipe_id = recipes.recipe_id

        WHERE recipe_details.recipe_id IN (
            SELECT recipe_id 
            FROM recipe_details
            JOIN ingredients ON recipe_details.ingredient = ingredients.ingredient_id
            WHERE ingredient_name IN ({})
            GROUP BY recipe_id
            HAVING COUNT(DISTINCT ingredient) >= ({})
        )
    """

    placeholders: str = ", ".join("?" * len(ingredients))
    query: str = query.format(placeholders, len(ingredients))
    recipe_df: pandas.DataFrame = pandas.read_sql(query, conn, params=ingredients)
    print("guac1")
    print(recipe_df[recipe_df["recipe_name"] == "Guacamole"])
    if len(recipe_df["recipe_name"].unique()) < 10:
        query: str = """
        SELECT main_query.recipe_id, recipe_name, ingredient_name, amount, unit_name, recipe_descriptions
        FROM (
            SELECT recipe_details.recipe_id
            FROM recipe_details
            JOIN ingredients ON recipe_details.ingredient = ingredients.ingredient_id
            WHERE ingredient_name IN ({})
            GROUP BY recipe_id
            ORDER BY COUNT(DISTINCT CASE WHEN ingredient_name IN ({}) THEN ingredient_name END) DESC
        ) AS main_query
        JOIN recipe_details ON main_query.recipe_id = recipe_details.recipe_id
        JOIN ingredients ON recipe_details.ingredient = ingredients.ingredient_id
        JOIN measurement_units ON recipe_details.measurement = measurement_units.unit_id
        JOIN recipes ON recipe_details.recipe_id = recipes.recipe_id
        LIMIT 1000
        """
        placeholders: str = ", ".join("?" * len(ingredients))
        query: str = query.format(placeholders, placeholders)
        recipe_df1: pandas.DataFrame = pandas.read_sql(
            query, conn, params=ingredients * 2
        )
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
    print(recipe_df[["ingredient_name", "amount", "unit_name"]])
    print(recipe_df["recipe_descriptions"].unique())

    conn.close()
    return recipe_df


get_data()
