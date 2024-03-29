import pandas as pd
import os


def replace_ingredients(ingredients: pd.DataFrame, recipes: pd.DataFrame):
    print("input ingredient id to be replaced")
    ingredient = int(input())
    print("input replacement")
    replacement = int(input())
    print("If description should be overwritten, input 'y', else input 'n'")
    overwrite = input()
    if overwrite == "y":
        print("input description")
        description = int(input())
    print("If measurement unit should be overwritten, input 'y', else input 'n'")
    overwrite1 = input()
    if overwrite1 == "y":
        print("input measurement unit")
        measurement = int(input())
    ingredient_count = ingredients.loc[
        ingredients["ID"] == ingredient, "ingredientcount"
    ].values[0]
    ingredients.loc[
        ingredients["ID"] == replacement, "ingredientcount"
    ] += ingredient_count
    ingredients.drop(ingredients[ingredients["ID"] == ingredient].index, inplace=True)

    if overwrite == "y":
        recipes.loc[recipes["ingredient"] == ingredient, "description"] = description
    if overwrite1 == "y":
        recipes.loc[recipes["ingredient"] == ingredient, "measurement"] = measurement
    recipes.loc[recipes["ingredient"] == ingredient, "ingredient"] = replacement

    return ingredients, recipes


ingredients = pd.read_csv("Output_files/ingredients_replaced.csv")
directory = "Output_files"
recipes = []
for filename in os.listdir(directory):
    if filename.startswith("recipedetails_replaced") and filename.endswith(".csv"):
        df = pd.read_csv(os.path.join(directory, filename))
        recipes.append(df)

recipes = pd.concat(recipes, ignore_index=True)

replace_ingredients(ingredients, recipes)
output_dir = "Output_files"
ingredients.to_csv("Output_files/ingredients_replaced.csv", index=False)
for i, df in enumerate(
    [recipes[i : i + 1000000] for i in range(0, len(recipes), 1000000)]
):
    df.to_csv(
        os.path.join(output_dir, "recipedetails_replaced" + str(i) + ".csv"),
        index=False,
    )
