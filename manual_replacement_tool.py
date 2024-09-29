"""OS imported"""
import os
import pandas as pd


def replace_ingredients(
    df_details: pd.DataFrame, df_ingredients: pd.DataFrame, ids: list, new_id: int
) -> tuple:
    """
    This function replaces specified ingredient IDs in the details DataFrame,
    and removes the corresponding rows from the ingredients DataFrame.

    Parameters:
    df_details (pd.DataFrame): The details DataFrame.
    df_ingredients (pd.DataFrame): The ingredients DataFrame.
    ids_to_replace (list): The list of IDs to be replaced.
    new_id (int): The new ID that will replace the old IDs.

    Returns:
    df_details (pd.DataFrame): The updated details DataFrame.
    df_ingredients (pd.DataFrame): The updated ingredients DataFrame.
    """
    # Replace the IDs in the 'recipedetails0' DataFrame
    df_details["ingredient"] = df_details["ingredient"].replace(ids, new_id)

    # Remove the replaced ingredients from the 'ingredients' DataFrame
    df_ingredients = df_ingredients[~df_ingredients["ID"].isin(ids)]

    return df_details, df_ingredients


ingredients = pd.read_csv("Output_files/ingredients_replaced.csv")
DIRECTORY = "Output_files"
recipes = []
filenames = sorted(os.listdir(DIRECTORY))
for filename in filenames:
    if filename.startswith("recipedetails_replaced") and filename.endswith(".csv"):
        df = pd.read_csv(os.path.join(DIRECTORY, filename))
        recipes.append(df)

recipes = pd.concat(recipes, ignore_index=True)

ids_to_replace: list = [
    0,
    118,
    275,
    326,
    369,
    391,
    395,
    411,
    425,
    457,
    563,
    649,
    654,
    750,
    768,
    852,
    896,
    1019,
    1073,
    1098,
    1227,
    1236,
    1284,
    1317,
    1359,
    1426,
    1442,
    1592,
    1660,
    1703,
    1706,
    1764,
    1774,
    2445,
    2448,
    2468,
    2629,
    2642,
    2660,
    3469,
    3543,
    3607,
    3616,
    3621,
    3861,
    4223,
    4286,
    4347,
    5005,
    5045,
    5220,
    5741,
    5868,
    6118,
    6370,
    6662,
    6788,
    6856,
    7547,
    8899,
    10539,
    10871,
    11018,
    12133,
    13093,
    13158,
    16333,
    17366,
]  # The IDs to be replaced
NEW_ID: int = 5923

recipes, ingredients = replace_ingredients(recipes, ingredients, ids_to_replace, NEW_ID)
ids_to_replace: list = [

]
NEW_ID: int = 

recipes, ingredients = replace_ingredients(recipes, ingredients, ids_to_replace, NEW_ID)

OUTPUT_DIR = "Output_files"
ingredients.to_csv("Output_files/ingredients_replaced.csv", index=False)
for i, df in enumerate(
    [recipes[i : i + 1000000] for i in range(0, len(recipes), 1000000)]
):
    df.to_csv(
        os.path.join(OUTPUT_DIR, "recipedetails_replaced" + str(i) + ".csv"),
        index=False,
    )
