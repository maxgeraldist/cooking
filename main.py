import time
importtimestart = time.time()
import pandas as pd
import sys
import os

from utils.tosql import into_sql_measurement_units

# Add the Functions directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "utils"))
from cleaner import recount_IDs, clean_data, refactor_ingredients, fill_ids
from json_parser import process_file 
from instructions import instructions
from tosql import into_sql_ingredients, into_sql_instructions, into_sql_measurement_units, into_sql_recipes
from measurement_units import measurement_units

print("Import time: {}".format(time.time() - importtimestart))

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
    dfs = []  # use a list instead of a dictionary
    for i, input_file in enumerate(input_files, start=1):
        rows = []
        df = process_file(input_file, rows)
        df, ingredients = clean_data(df, ingredients,output_dir)
        dfs.append(df)  # append the dataframe to the list
        print(str(i) + " files processed")
        df = None
    result = pd.concat(dfs)  # concatenate the list of dataframes
    result, id_map = recount_IDs(result, id_map)
    result, ingredients = refactor_ingredients(result, ingredients, n)
    result = fill_ids(result,output_dir)
    # save results to multiple .csv files, one per 1m rows
    for i, df in enumerate(
        [result[i : i + 1000000] for i in range(0, len(result), 1000000)]
    ):
        df.to_csv(os.path.join(output_dir, "output" + str(i) + ".csv"), index=False)
    ingredients.to_csv(os.path.join(output_dir,"ingredients" + ".csv"), index=False)
    instructions.to_csv(os.path.join(output_dir,"instructions" + ".csv"), index=False)
    print("Parsing complete.")
    print("Time taken: {}".format(time.time() - start_time))
    user = input("Enter the username for the SQL server: ")
    pw = input("Enter the password for the SQL server: ")
    print("Writing to SQL...")
    into_sql_ingredients(ingredients,user,pw)
    into_sql_instructions(instructions,user,pw)
    into_sql_measurement_units(measurement_units,user,pw)
    into_sql_recipes(result,user,pw)


main()
