import pandas as pd
import json
import re
import time 
start_time = time.time()
def process_file(filename, df):
    with open(filename, "r") as f:
        data = json.load(f)

    # Define a list of common measurement units
    measurement_units = ["cup", "cups", "teaspoon", "teaspoons", "tablespoon", "tablespoons", "ounce", "ounces", "pound", "pounds"]

    # Create an empty list to store the data for each row
    rows = []

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
        amount = parts[0].where(parts[0].str.isdigit(), "")
        measurement = parts[1].where(parts[1].isin(measurement_units), "")
        ingredient_name = parts[2:].apply(lambda x: ' '.join(x.dropna()), axis=1).where(measurement != "", parts[1:].apply(lambda x: ' '.join(x.dropna()), axis=1))

        # Create a DataFrame from the extracted data
        data = {
            "recipe_id": [recipe_id] * len(ingredients),
            "ingredient": ingredient_name,
            "amount": amount,
            "measurement": measurement,
            "instructions": [instructions] * len(ingredients),
            "recipe_name": [recipe_name] * len(ingredients)
        }
        new_df = pd.DataFrame(data)

        # Concatenate the new DataFrame with the existing DataFrame
        df = pd.concat([df, new_df], ignore_index=True)

    return df# Create an empty DataFrame with the desired columns
df = pd.DataFrame(columns=["recipe_id", "ingredient", "amount", "measurement", "instructions","recipe_name"])

# Process each input file
df = process_file("recipes_raw_nosource_ar.json", df)
df = process_file("recipes_raw_nosource_epi.json", df)
df = process_file(".json", df)

# Export the DataFrame to an Excel file
df.to_excel("recipes.xlsx", index=False)# Export the DataFrame to an Excel file
end_time = time.time()
print("Time taken to run this cell :", end_time - start_time, "seconds.")
