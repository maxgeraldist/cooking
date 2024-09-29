import json
import re
import pandas as pd
from utils.measurement_units import measurement_units

FRACTION_PATTERN = re.compile(r"(\d+)/(\d+)")
TO_TASTE_PATTERN = re.compile(r"to taste", re.IGNORECASE)
RANGE_PATTERN = re.compile(r"(\d+) to (\d+)")
CLEANING_PATTERN = re.compile(r"\([^)]*\)|advertisement", re.IGNORECASE)


fraction_map = str.maketrans(
    {
        "⅔": "2/3",
        "⅓": "1/3",
        "¼": "1/4",
        "½": "1/2",
        "¾": "3/4",
        "⅛": "1/8",
        "⅜": "3/8",
        "⅝": "5/8",
        "⅞": "7/8",
    }
)


def redo_fractions(string):
    return string.translate(fraction_map)


def get_numerator_denominator(string):
    """
    The function takes a string, containing an m/n fraction, and performs a calculated float f=m/n
    """
    match = FRACTION_PATTERN.search(string)
    return (int(match.group(1)), int(match.group(2))) if match else (None, None)


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

    row = redo_fractions(row).replace("-", " ").strip()
    if not row:
        return pd.Series([amount, measurement, ingredient_name])
    row = TO_TASTE_PATTERN.sub("", row)

    numerator, denominator = get_numerator_denominator(row)
    if numerator and denominator:
        fraction = numerator / denominator
        row = FRACTION_PATTERN.sub("", row)

    match_range = RANGE_PATTERN.search(row)
    if match_range:
        amount = (float(match_range.group(1)) + float(match_range.group(2))) / 2
        row = RANGE_PATTERN.sub("", row)
    words = row.split()

    for word in words[:]:
        if word in measurement_units:
            measurement = word
            words.remove(word)

    if words and words[0].lower() in number_words:
        amount = float(number_words[words[0].lower()])
        words.pop(0)
    elif words and words[0].isdigit():
        amount = int(words[0]) + fraction
        words.pop(0)
    else:
        amount = fraction if fraction else 1

    ingredient_name = " ".join(words)
    if not ingredient_name and not measurement and not amount:
        return "", "", ""
    else:
        return amount, measurement, ingredient_name


def process_file(filename):
    print(f"Processing file: {filename}...")

    with open(filename, "r") as f:
        raw_data = f.read()

    # Step 2: Remove semicolons
    cleaned_data = raw_data.replace(";", "")

    # Step 3: Parse the cleaned data as JSON
    data = json.loads(cleaned_data)
    rows = []
    for recipe_id, recipe_data in data.items():
        recipe_name = recipe_data.get("title", "")
        if not recipe_name:
            continue

        recipe_name = recipe_name.replace(";", "")
        instructions = recipe_data.get("instructions", "")
        if not instructions:
            continue
        instructions = instructions.replace(";", "")
        ingredients_list = recipe_data.get("ingredients", [])

        if not ingredients_list:
            continue

        # Collect all ingredient data into a single list
        for ingredient in ingredients_list:
            # Prepare a single dictionary for each ingredient
            rows.append(
                {
                    "recipe_id": recipe_id,
                    "recipe_name": recipe_name,
                    "descriptions": instructions,
                    "ingredient": ingredient.lower(),
                }
            )
    df = pd.DataFrame(rows)
    df["ingredient"] = df["ingredient"].str.replace(CLEANING_PATTERN, "", regex=True)
    df = df[df["ingredient"] != ""]  # Remove empty ingredients
    ingredient_data = df["ingredient"].apply(
        lambda x: extract_ingredient_data(x, measurement_units)
    )
    ingredient_df = pd.DataFrame(
        ingredient_data.tolist(), columns=pd.Index(["amount", "measurement", "ingredient_name"])
    )
    ingredient_df["ingredient"] = ingredient_df["ingredient_name"]
    # Combine ingredient data with the main DataFrame
    df = df.reset_index(drop=True)
    final_df = pd.concat(
        [
            df.drop(columns=["ingredient"]),
            ingredient_df.drop(columns=["ingredient_name"]),
        ],
        axis=1,
    )
    final_df = final_df[final_df["ingredient"] != ""]
    return final_df
