import json
import re
import time
import os
import pandas as pd
from measurement_units import measurement_units
start_time = time.time()
# Rewrite symbols like '⅔' (unicode fraction) in a way that can be evaluated
def redo_fractions(string):
    string = string.replace('⅔', '2/3')
    string = string.replace('⅓', '1/3')
    string = string.replace('¼', '1/4')
    string = string.replace('½', '1/2')
    string = string.replace('¾', '3/4')
    string = string.replace('⅛', '1/8')
    string = string.replace('⅜', '3/8')
    string = string.replace('⅝', '5/8')
    string = string.replace('⅞', '7/8')
    return string
 
# Get the numerator and denominator of a fraction
def get_numerator_denominator(string):
    match = re.search(r'(\d+)/(\d+)', string)
    if match:
        numerator = int(match.group(1))
        denominator = int(match.group(2))
        return numerator, denominator
    return None, None

number_words = {'one': '1', 'two': '2', 'three': '3', 'four': '4', 'five': '5',
                    'six': '6', 'seven': '7', 'eight': '8', 'nine': '9', 'ten': '10'}
def extract_ingredient_data(row, measurement_units):
    amount = 0
    measurement = ""
    ingredient_name = ""
    fraction = 0
    row = redo_fractions(row)
    row = row.replace('-', ' ')
    row = row.strip()
    if row is none:
        return pd.series([amount, measurement, ingredient_name])
    if 'to taste' in row:
        row = re.sub('to taste', '', row)
    if '/' in row:
        numerator, denominator = get_numerator_denominator(row)
        fraction = float(float(numerator) / float(denominator)) if denominator is not none else 0
        row = re.sub((str(numerator) + '/' + str(denominator)), '', row)
    elif 'to' in row:
        match1 = re.search(r'(\d+) to (\d+)', row)
        if match1:
            amount = (float(match1.group(1))+float(match1.group(2)))/2
            row = re.sub((str(match1.group(1)) + ' to ' + str(match1.group(2))), '', row)
    match2 = re.search(r'(\d+)', row)
    if match2:
        amount = float(match2.group(1))
        row = re.sub(str(match2.group(1)), '', row)
    words = row.split(" ")
    new_words = []
    for word in words[:]:
        if word in measurement_units:
            measurement = word
            words.remove(word)
    if words and words[0] in number_words:
        amount = number_words[words[0]]
        words.remove(words[0])
    if words and words[0].isdigit():
        amount = int(words[0])+fraction
        words.remove(words[0])
    elif words and fraction != 0:
        amount = float(fraction)
    else:
        amount = 1
    ingredient_name = " ".join(words)
    return pd.series([amount, measurement, ingredient_name])
# define the measurement units we want to extract
def process_file(filename, rows): 
    with open(filename, "r") as f:
        # read the contents of the input file into a string
        file_contents = f.read()
        file_contents = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', file_contents)
        # parse the modified string using json.loads
        data = json.loads(file_contents) 
        # iterate over the data and extract the information
    for recipe_id, recipe_data in data.items():
        instructions = recipe_data.get("instructions", "")     
        recipe_name = recipe_data.get("title", "")
        ingredients_list = recipe_data.get("ingredients", [])
        if not ingredients_list :
            continue
        ingredients = pd.series(ingredients_list)
        ingredients = ingredients.str.replace('\"advertisement\"', '', regex=true)
        ingredients = ingredients.str.replace(r'\([^)]*\)|advertisement', '', regex=true)
        ingredients = ingredients.str.lower()
        #remove empty rows
        ingredients = ingredients[ingredients != '']
        # extract the amount, measurement, and ingredient name from the parts 
        if any(elem is not none for elem in recipe_data):
            data = ingredients.apply(lambda x: extract_ingredient_data(x, measurement_units))
        data.columns = ["amount", "measurement", "ingredient_name"]
        amount = data["amount"]
        measurement = data["measurement"]
        ingredient_name = data["ingredient_name"]
        # create a dataframe from the extracted data
        data = {
            "recipe_id": recipe_id,
            "recipe_name": recipe_name,
            "ingredient": ingredient_name,
            "amount": amount,
            "measurement": measurement,
            "instructions": instructions,
        }
        new_df = pd.dataframe(data)
        # append all rows to the rows list
        rows.extend(new_df.to_dict('records'))
i = 0
csv_or_xlsx = input("press 1 for csv or 2 for xlsx outputs: ")
# remap the input to the correct file extension

input_dir = 'Input_files'
input_files = [os.path.join(input_dir, name) for name in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, name))]
# Process each input file separately (the dataframe is too big for the excel file format))
for i, input_file in enumerate(input_files, start=1):
    rows = []
    process_file(input_file, rows)
    print("Number of rows in the dataframe: ", len(rows))
    df = pd.DataFrame(rows)
    if csv_or_xlsx == "1":
        df.to_csv(("output" + str(i) + ".csv"), index=False)
    elif csv_or_xlsx == "2":
        df.to_excel(("output" + str(i) + ".xlsx"), index=False)
    print(str(i)+" files processed")
    df = None
print("Total time: %s seconds" % (time.time() - start_time))
