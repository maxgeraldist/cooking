import json
import re
import pandas as pd
from .measurement_units import measurement_units


# Rewrite symbols like '⅔' (unicode fraction) in a way that can be evaluated
def redo_fractions(string):
    string = string.replace("⅔", "2/3")
    string = string.replace("⅓", "1/3")
    string = string.replace("¼", "1/4")
    string = string.replace("½", "1/2")
    string = string.replace("¾", "3/4")
    string = string.replace("⅛", "1/8")
    string = string.replace("⅜", "3/8")
    string = string.replace("⅝", "5/8")
    string = string.replace("⅞", "7/8")
    return string


# Get the numerator and denominator of a fraction
def get_numerator_denominator(string):
    match = re.search(r"(\d+)/(\d+)", string)
    if match:
        numerator = int(match.group(1))
        denominator = int(match.group(2))
        return numerator, denominator
    return None, None


number_words = {
    "one": "1",
    "two": "2",
    "three": "3",
    "four": "4",
    "five": "5",
    "six": "6",
    "seven": "7",
    "eight": "8",
    "nine": "9",
    "ten": "10",
}


def extract_ingredient_data(row, measurement_units):
    amount = 0
    measurement = ""
    ingredient_name = ""
    fraction = 0
    row = redo_fractions(row)
    row = row.replace("-", " ")
    row = row.strip()
    if row is None:
        return pd.Series([amount, measurement, ingredient_name])
    if "to taste" in row:
        row = re.sub("to taste", "", row)
    if "/" in row:
        numerator, denominator = get_numerator_denominator(row)
        fraction = (
            float(float(numerator) / float(denominator))
            if denominator is not None
            else 0
        )
        row = re.sub((str(numerator) + "/" + str(denominator)), "", row)
    elif "to" in row:
        match1 = re.search(r"(\d+) to (\d+)", row)
        if match1:
            amount = (float(match1.group(1)) + float(match1.group(2))) / 2
            row = re.sub(
                (str(match1.group(1)) + " to " + str(match1.group(2))), "", row
            )
    words = row.split(" ")

    for word in words[:]:
        if word in measurement_units:
            measurement = word
            words.remove(word)
    if words and words[0] in number_words:
        amount = number_words[words[0]]
        words.remove(words[0])
    elif words and words[0].isdigit():
        amount = int(words[0]) + fraction
        words.remove(words[0])
    elif words and fraction != 0:
        amount = float(fraction)
    else:
        amount = 1
    ingredient_name = " ".join(words)
    return pd.Series([amount, measurement, ingredient_name])


def process_file(filename, rows):
    print("Processing file: " + filename + "...")
    with open(filename, "r") as f:
        file_contents = f.read()
        file_contents = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", file_contents)
        data = json.loads(file_contents)
    for recipe_id, recipe_data in data.items():
        ingredients_list = recipe_data.get("ingredients", [])
        if not ingredients_list:
            continue
        ingredients_list = [
            ingredient if ";" not in ingredient else '"' + ingredient + '"'
            for ingredient in ingredients_list
        ]
        instructions = recipe_data.get("instructions", "")
        if instructions is not None and ";" in instructions:
            instructions = f'"{instructions}"'
        recipe_name = recipe_data.get("title", "")
        if recipe_name is not None and ";" in recipe_name:
            recipe_name = f'"{recipe_name}"'
        ingredients = pd.Series(ingredients_list)
        ingredients = ingredients.str.lower()
        ingredients = ingredients.str.replace('"advertisement"', "", regex=True)
        ingredients = ingredients.str.replace(
            r"\([^)]*\)|advertisement", "", regex=True
        )
        ingredients = ingredients[ingredients != ""]
        if any(elem is not None for elem in recipe_data):
            data = ingredients.apply(
                lambda x: extract_ingredient_data(x, measurement_units)
            )
        data.columns = ["amount", "measurement", "ingredient_name"]
        amount = data["amount"]
        measurement = data["measurement"]
        ingredient_name = data["ingredient_name"]
        data = {
            "recipe_id": recipe_id,
            "recipe_name": recipe_name,
            "ingredient": ingredient_name,
            "amount": amount,
            "measurement": measurement,
            "instructions": instructions,
        }
        new_df = pd.DataFrame(data)
        rows.extend(new_df.to_dict("records"))
    df = pd.DataFrame(rows)
    return df
