import time
import pandas as pd
import sys
import os

# Add the Functions directory to the Python path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "Functions"))
from cleaner import preclean, recount_IDs, clean_data, refactor_ingredients
from json_parser import process_file

input_dir = "Input_files"
output_dir = "Output_files"
input_files = [
    os.path.join(input_dir, name)
    for name in os.listdir(input_dir)
    if os.path.isfile(os.path.join(input_dir, name))
]
output_files = [
    os.path.join(output_dir, name)
    for name in os.listdir(output_dir)
    if os.path.isfile(os.path.join(output_dir, name))
]

csv_or_xlsx = input("Enter 1 for csv or 2 for xlsx: ")
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
        df, id_map = recount_IDs(df, id_map)
        df = preclean(df)
        df, ingredients = clean_data(df, ingredients, n)
        dfs.append(df)  # append the dataframe to the list
        print("Number of rows in the dataframe: ", len(rows))
        print(str(i) + " files processed")
        df = None
    result = pd.concat(dfs)  # concatenate the list of dataframes
    print('Number of rows in the dataframe: ', len(result))
    result, ingredients = refactor_ingredients(result, ingredients, n)
    print("Number of rows in the dataframe: ", len(result))
    if csv_or_xlsx == "1":
        result.to_csv(("output" + ".csv"), index=False)
        ingredients.to_csv(("ingredients" + ".csv"), index=False)
    elif csv_or_xlsx == "2":
        result.to_excel(("output" + ".xlsx"), index=False)
        ingredients.to_excel(("ingredients" + ".xlsx"), index=False)
    print("Done!")
    print("Time taken: {}".format(time.time() - start_time))


main()
