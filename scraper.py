import pandas as pd
import json
import re
import time 
start_time = time.time()
def extract_ingredient_data(row, measurement_units):
    amount = ""
    measurement = ""
    ingredient_name = ""
    if row[0].isdigit():
        amount = row[0]
        if row[1] in measurement_units:
            measurement = row[1]
            ingredient_name = ' '.join(row[2:])
        else:
            ingredient_name = ' '.join(row[1:])
    else:
        ingredient_name = ' '.join(row)
    return pd.Series([amount, measurement, ingredient_name])
# Define the measurement units we want to extract
measurement_units = ["cup", "cups", "teaspoon", "teaspoons", "tablespoon", "tablespoons", "ounce", "ounces", "pound", "pounds"]

def process_file(filename, rows):
    with open(filename, "r") as f:
        data = json.load(f)
    # Iterate over the data and extract the information
    for recipe_id, recipe_data in data.items():
        instructions = recipe_data["instructions"]
        recipe_name = recipe_data["title"]
        ingredients = pd.Series(recipe_data["ingredients"])
        # Remove text in parentheses
        ingredients = ingredients.str.replace(r'\([^)]*\)|ADVERTISEMENT', '', regex=True)

        # Split the ingredient strings into parts
        parts = ingredients.str.split(expand=True)

        # Extract the amount, measurement, and ingredient name from the parts
        data = parts.apply(lambda row: extract_ingredient_data(row, measurement_units), axis=1)
        data.columns = ["amount", "measurement", "ingredient_name"]
        amount = data["amount"]
        measurement = data["measurement"]
        ingredient_name = data["ingredient_name"]        # Create a DataFrame from the extracted data
        data = {
            "recipe_id": [recipe_id] * len(ingredients),
            "ingredient": ingredient_name,
            "amount": amount,
            "measurement": measurement,
            "instructions": [instructions] * len(ingredients),
            "recipe_name": [recipe_name] * len(ingredients)
        }
        new_df = pd.DataFrame(data)

        # Append the new data to the rows list
        rows.extend(new_df.to_dict('records'))

# Create an empty list to store the data for each row
rows = []

# Process each input file
process_file("recipes_raw_nosource_ar.json", rows)
process_file("recipes_raw_nosource_epi.json", rows)
process_file("recipes_raw_nosource_fn.json", rows)
df = pd.DataFrame(rows)
# Export the DataFrame to an Excel file
df.to_excel("recipes.xlsx", index=False)# Export the DataFrame to an Excel file
end_time = time.time()
print("Time taken to run this cell :", end_time - start_time, "seconds.")
