import pandas as pd
import json
import re
import time
import os

def get_numerator_denominator(string):
    match = re.search(r'(\d+)/(\d+)', string)
    if match:
        return match.group(1), match.group(2)
    return None, None

start_time = time.time()
def extract_ingredient_data(row, measurement_units):
    amount = ""
    measurement = ""
    ingredient_name = ""
    if '/' in row[0]:
        numerator, denominator = get_numerator_denominator(row[0])
        fraction = float(numerator) / float(denominator) if numerator is not None and denominator is not None else 0
        row[0] = re.sub(r'(\d+)/(\d+)', '', row[0])
    words = row[0].split(" ")
    for word in words:
        word=word.replace("-"," ")
        if word in measurement_units:
            measurement = word
    if words[0].isdigit():
        amount = words[0]
        ingredient_name = " ".join(words[1:])
    elif words[0].isdigit() == False and measurement == "":
        amount = "1"
        ingredient_name = " ".join(words)
 
    return pd.Series([amount, measurement, ingredient_name])# Define the measurement units we want to extract
measurement_units = ["cup", "cups", "teaspoon", "teaspoons", "tablespoon", "tablespoons", "ounce","-ounce","-ounces", "ounces", "pound", "pounds", "clove", "cloves", "inch", "inches"]
def process_file(filename, rows): 
    with open(filename, "r") as f:
        # Read the contents of the input file into a string
        file_contents = f.read()
        # Remove or escape any invalid control characters
        file_contents = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', file_contents)
        # Parse the modified string using json.loads
        data = json.loads(file_contents) 
        # Iterate over the data and extract the information
    for recipe_id, recipe_data in data.items():
        instructions = recipe_data.get("instructions", "")
        recipe_name = recipe_data.get("title", "")
        ingredients_list = recipe_data.get("ingredients", [])
        if not ingredients_list :
            continue
        ingredients = pd.Series(ingredients_list)
        ingredients = ingredients.str.replace('\"ADVERTISEMENT\"', '', regex=True)
        ingredients = ingredients.str.replace(r'\([^)]*\)|ADVERTISEMENT', '', regex=True)
        parts = ingredients.str.split(expand=True)
        # Extract the amount, measurement, and ingredient name from the parts 
        data = parts.apply(lambda row: extract_ingredient_data(row, measurement_units), axis=1)
        data.columns = ["amount", "measurement", "ingredient_name"]
        amount = data["amount"]
        measurement = data["measurement"]
        ingredient_name = data["ingredient_name"]
        # Create a DataFrame from the extracted data
        data = {
            "recipe_id": [instructions if i == 0 else "" for i in range(len(ingredients))],
            "ingredient": ingredient_name,
            "amount": amount,
            "measurement": measurement,
            "instructions": [instructions if i == 0 else "" for i in range(len(ingredients))],
            "recipe_name": [recipe_name if i == 0 else "" for i in range(len(ingredients))]
        }
        new_df = pd.DataFrame(data)
        # Append all rows to the rows list
        rows.extend(new_df.to_dict('records'))# Create an empty list to store the data for each row
rows = []
i = 0
# Loop through each input file and process it

input_dir = 'Input_files'
input_files = [os.path.join(input_dir, name) for name in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, name))]
# Process each input file separately (the dataframe is too big for the excel file format))
for i, input_file in enumerate(input_files, start=1): 
    process_file(input_file, rows)
    df = pd.DataFrame(rows)
    print(str(i)+" files processed")
    df.to_excel("recipes"+str(i)+".xlsx", index=False)
    rows = []
    end_time = time.time()
print("Time taken to run this cell :", end_time - start_time, "seconds.")
print("Number of errors: ", error_count)
