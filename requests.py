import pyodbc
import pandas


def get_data():
    conn = pyodbc.connect(
        "DRIVER={MySQL};SERVER=localhost;DATABASE=Cooking;USER="
        + input("Enter username: ")
        + ";PASSWORD="
        + input("Enter password: ")
    )
    print("Type the ingredients would like to use; Type 'STOP' to end the query")
    ingredient_request = ""
    ingredients = []
    while ingredient_request != "STOP":
        ingredient_request = input("Ingredient: ")
        ingredients.append(ingredient_request)
    ingredients.pop()

    # Get ingredient_ids for the input ingredients
    ingredient_ids = []
    for ingredient in ingredients:
        query = (
            "SELECT ingredient_id FROM ingredients WHERE ingredient_name = '"
            + ingredient
            + "'"
        )
        ingredient_id_df = pandas.read_sql(query, conn)
        ingredient_ids.extend(ingredient_id_df["ingredient_id"].tolist())

    # Query recipes table with ingredient_ids
    query = "SELECT * FROM recipes WHERE "
    for ingredient_id in ingredient_ids:
        query += "ingredient LIKE '%" + str(ingredient_id) + "%' AND "
    query = query[:-5]
    recipe_df = pandas.read_sql(query, conn)
    return recipe_df
