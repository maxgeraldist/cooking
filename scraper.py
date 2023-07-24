import pandas as pd
import json
import re
import time


start_time = time.time()
def extract_ingredient_data(row, measurement_units):
    amount = ""
    measurement = ""
    ingredient_name = ""
    # Check if the row contains any non-None elements
    if any(elem is not None for elem in row):
        # Check if the first element contains a number
        match = re.search(r'\d', row[0])
        if match:
            # Extract the amount from the first element
            amount_str = row[0]
            # Check if the amount contains a fraction
            if '/' in amount_str:
                # Convert the fractional part to a float
                numerator, denominator = amount_str.split('/')
                frac_part = float(numerator) / float(denominator)
                # Check if there is a whole part
                if len(row) > 1 and row[1] and row[1][0].isdigit():
                    # Extract the whole part from the second element
                    whole_part = float(row[1])
                    # Add the whole and fractional parts together
                    amount = whole_part + frac_part
                    # Shift the row to the left by one element
                    row = row[1:]
                else:
                    # Set the amount to the fractional part
                    amount = frac_part
            else:
                # Check if the amount contains a measurement unit
                for unit in measurement_units:
                    if unit in amount_str:
                        # Split the amount string into amount and measurement parts
                        amount_part, measurement_part = amount_str.split(unit)
                        # Convert the amount part to a float
                        try:
                            amount = float(amount_part)
                        except ValueError:
                            break
                        # Set the measurement to the extracted unit
                        measurement = unit
                        break
                else:
                    try:
                        # Convert the amount to a float
                        amount = float(amount_str)
                    except ValueError:
                        return pd.Series([None, None, None])
            if not measurement:
                if len(row) > 1 and row[1] in measurement_units:
                    measurement = row[1]
                    ingredient_name = ' '.join(filter(None, row[2:]))
                else:
                    ingredient_name = ' '.join(filter(None, row[1:]))
        else:
            ingredient_name = ' '.join(filter(None, row))
    return pd.Series([amount, measurement, ingredient_name])# Define the measurement units we want to extract
measurement_units = ["cup", "cups", "teaspoon", "teaspoons", "tablespoon", "tablespoons", "ounce","-ounce","-ounces", "ounces", "pound", "pounds", "clove", "cloves", "inch", "inches"]

def process_file(filename, rows):
    with open(filename, "r") as f:
        # Read the contents of the input file into a string
        file_contents = f.read()
        # Remove or escape any invalid control characters
        file_contents = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', file_contents)
        # Parse the modified string using json.loads
        data = json.loads(file_contents)    # Iterate over the data and extract the information
    for recipe_id, recipe_data in data.items():
        instructions = recipe_data.get("instructions", "")
        recipe_name = recipe_data.get("title", "")
        ingredients_list = recipe_data.get("ingredients", [])
        if not ingredients_list :
            continue
        ingredients = pd.Series(ingredients_list)
        # Remove text in parentheses
        ingredients = ingredients.str.replace('\"ADVERTISEMENT\"', '', regex=True)
        ingredients = ingredients.str.replace(r'\([^)]*\)|ADVERTISEMENT', '', regex=True)
        # Split the ingredient strings into parts
        parts = ingredients.str.split(expand=True)
        # Extract the amount, measurement, and ingredient name from the parts
        data = parts.apply(lambda row: extract_ingredient_data(row, measurement_units), axis=1)
        data.columns = ["amount", "measurement", "ingredient_name"]
        amount = data["amount"]
        measurement = data["measurement"]
        ingredient_name = data["ingredient_name"]
        # Create a DataFrame from the extracted data
        data = {
            "recipe_id": [recipe_id] * len(ingredients),
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
i = 1
error_count=0
input_files = ["recipes1.json", "recipes2.json", "recipes3.json"]
# Process each input file separately (the dataframe is too big for the excel file format))
for input_file in input_files:
    try:
        process_file(input_file, rows)
        df = pd.DataFrame(rows)
        i += 1
        print(str(i)+" files processed")
        df.to_excel("recipes"+str(i)+".xlsx", index=False)
        rows = []
    except:
        error = "Error in file "+input_file
        print(error)
        error_count+=1
        print("Error count: "+str(error_count))
        continue
end_time = time.time()
print("Time taken to run this cell :", end_time - start_time, "seconds.")
