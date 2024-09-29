"""The main file for taking the json files with recipes and outputting a MySQL database"""

import time

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from utils.cleaner import recount_IDs, clean_data, refactor_ingredients, fill_ids
from utils.json_parser import process_file
from utils.descriptions import descriptions
from utils.tosql import (
    into_sql_ingredients,
    into_sql_descriptions,
    into_sql_measurement_units,
    into_sql_recipedetails,
    into_sql_recipes,
)
from utils.measurement_units import measurement_units

INPUT_DIR = "Input_files"
OUTPUT_DIR = "Output_files"
input_files = [
    os.path.join(INPUT_DIR, name)
    for name in os.listdir(INPUT_DIR)
    if os.path.isfile(os.path.join(INPUT_DIR, name))
]

n = int(
    input("Enter how aggressive you want the refactoring to be (1-10 recommended): ")
)


def main():
    """
    The main function, that takes json files with parsed recipes,
    cleans strings, parses numeric and categorical information
    and plugs them into a mysql database.
    """
    id_map = {}
    start_time = time.time()
    print("Processing files...")
    ingredients = pd.DataFrame(
        columns=pd.Index(["ID", "ingredient", "ingredientcount"])
    )
    dfs = []
    for i, input_file in enumerate(input_files, start=1):
        df = process_file(input_file)
        df, ingredients = clean_data(df, ingredients, OUTPUT_DIR)
        dfs.append(df)
        print(str(i) + " files processed")
        df = None
    result = pd.concat(dfs)
    result, id_map = recount_IDs(result, id_map)
    result, ingredients = refactor_ingredients(result, ingredients, n)
    result = result.drop(columns="ID")
    result, ids = fill_ids(result, OUTPUT_DIR)
    result["measurement"] = result["measurement"].replace(np.nan, 9999).astype(int)
    result["description_ID"] = (
        result["description_ID"].replace([np.nan, None], 9999).astype(int)
    )
    for i, df in enumerate(
        [result[i : i + 1000000] for i in range(0, len(result), 1000000)]
    ):
        df.to_csv(
            os.path.join(OUTPUT_DIR, "recipedetails" + str(i) + ".csv"), index=False
        )
    ingredients.to_csv(os.path.join(OUTPUT_DIR, "ingredients" + ".csv"), index=False)
    descriptions.to_csv(os.path.join(OUTPUT_DIR, "descriptions" + ".csv"), index=False)
    print("Parsing complete.")
    print(f"Time spent cleaning: {time.time() - start_time}")
    try:
        with open("login.txt", "r", encoding="utf-8") as f:
            user, pw = f.read().splitlines()
    except FileNotFoundError:
        user: str = input("Enter the username for the SQL server: ")
        pw: str = input("Enter the password for the SQL server: ")
        if input("Save username and password to login.txt? (y/n): ") == "y":
            with open("login.txt", "w", encoding="utf-8") as f:
                f.write(user + "\n" + pw)
    conn = create_engine("mysql+pymysql://" + user + ":" + pw + "@localhost/Cooking")
    print("Writing to SQL...")
    start_time = time.time()
    valid_ids_set = set(ids["recipe_id"].unique())
    mask = result["recipe_id"].isin(valid_ids_set)
    result = result[mask]
    into_sql_ingredients(ingredients, conn)
    into_sql_descriptions(descriptions, conn)
    into_sql_measurement_units(measurement_units, conn)
    ids.drop_duplicates(subset=["recipe_id"], inplace=True)
    into_sql_recipes(ids, conn)
    into_sql_recipedetails(result, conn)
    print("SQL writing complete.")
    print(f"Time spent inserting into SQL: {time.time() - start_time}")


main()
