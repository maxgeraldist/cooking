import time

import pandas as pd
import os
import numpy as np

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


input_dir = "Input_files"
output_dir = "Output_files"
input_files = [
    os.path.join(input_dir, name)
    for name in os.listdir(input_dir)
    if os.path.isfile(os.path.join(input_dir, name))
]

i = 1
n = int(
    input("Enter how aggressive you want the refactoring to be (1-10 recommended): ")
)
dfs = []


def main():
    id_map = {}
    start_time = time.time()
    print("Processing files...")
    ingredients = pd.DataFrame(columns=["ID", "ingredient", "ingredientcount"])
    dfs = []
    for i, input_file in enumerate(input_files, start=1):
        rows = []
        df = process_file(input_file, rows)
        df, ingredients = clean_data(df, ingredients, output_dir)
        dfs.append(df)
        print(str(i) + " files processed")
        df = None
    result = pd.concat(dfs)  # concatenate the list of dataframes
    result, id_map = recount_IDs(result, id_map)
    result, ingredients = refactor_ingredients(result, ingredients, n)
    result = fill_ids(result, output_dir)
    # Maybe can be replaced with DEFAULT 9999 in SQL
    result["measurement"] = result["measurement"].replace(np.nan, 9999).astype(int)
    result["instruction_ID"] = (
        result["instruction_ID"].replace(np.nan, 9999).astype(int)
    )
    # save results to multiple .csv files, one per 1m rows
    for i, df in enumerate(
        [result[i : i + 1000000] for i in range(0, len(result), 1000000)]
    ):
        df.to_csv(
            os.path.join(output_dir, "recipedetails" + str(i) + ".csv"), index=False
        )
    ingredients.to_csv(os.path.join(output_dir, "ingredients" + ".csv"), index=False)
    descriptions.to_csv(os.path.join(output_dir, "descriptions" + ".csv"), index=False)
    print("Parsing complete.")
    print("Time spent cleaning: {}".format(time.time() - start_time))
    try:
        with open("login.txt") as f:
            user, pw = f.read().splitlines()
    except:
        user: str = input("Enter the username for the SQL server: ")
        pw: str = input("Enter the password for the SQL server: ")
        savevar = input("Save username and password to login.txt? (y/n): ")
        if savevar == "y":
            with open("login.txt", "w") as f:
                f.write(user + "\n" + pw)
    print("Writing to SQL...")
    insert_time = time.time()
    into_sql_ingredients(os.path.join(output_dir, "ingredients" + ".csv"), user, pw)
    into_sql_descriptions(os.path.join(output_dir, "descriptions" + ".csv"), user, pw)
    into_sql_measurement_units(measurement_units, user, pw)
    into_sql_recipedetails(result, user, pw)
    into_sql_recipes(os.path.join(output_dir, "recipe_id.csv"), user, pw)
    print("SQL writing complete.")
    print("Time spent writing to SQL: {}".format(time.time() - insert_time))


main()
