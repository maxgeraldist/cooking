"""File that contains the functions to clean the data in the dataframe after it
has been parsed from the json file, as well as those to refactor """

import re
import os
import pandas as pd
import numpy as np
from .refactor import Trie
from .descriptions import descriptions
from .measurement_units import measurement_units
from .useless_words import useless_words


def remove_after_for(s: str) -> str:
    idx = s.find(" for")
    return s[:idx] if idx != -1 and not s.endswith(":") else s


def remove_after_or(s: str) -> str:
    idx = s.find(" or more")
    return s[:idx] if idx != -1 else s


def remove_before_semicolon_in_middle(s: str) -> str:
    idx = s.find(";")
    return s[idx + 1 :] if idx != -1 and not s.endswith(":") else s


def remove_after_and(s: str) -> str:
    idx = s.find(" and ")
    return s[:idx] if idx != -1 else s


def remove_after_about(s: str) -> str:
    idx = s.find("about ")
    if idx != -1 and not s.startswith("about"):
        return s[:idx]
    return s[5:] if s.startswith("about") else s


def preclean(df: pd.DataFrame) -> pd.DataFrame:
    print("Precleaning...")

    regex_patterns = {
        "useless": "|".join(map(re.escape, useless_words)),
        "punctuation": r"[,.()]",
        "leading_of": r"^of ",
        "leading_plus": r"^plus ",
        "plus_more": r"plus more.*",
        "if_condition": r"\s*if.*",
        "such_as": r" such as .*",
    }

    df = df[df["ingredient"].notna()]

    df = df.assign(
        ingredient=df["ingredient"]
        .str.lower()
        .replace(regex_patterns["useless"], "", regex=True)
    )

    df = df.assign(
        ingredient=df["ingredient"]
        .str.replace(regex_patterns["punctuation"], "", regex=True)
        .str.strip()
    )

    df = df[~df["ingredient"].str.startswith("*")]
    df = df[~df["ingredient"].str.startswith("ingredient info")]
    df["ingredient"] = df["ingredient"].apply(remove_after_and)
    df["ingredient"] = df["ingredient"].apply(remove_before_semicolon_in_middle)
    df["ingredient"] = df["ingredient"].apply(remove_after_or)
    df["ingredient"] = df["ingredient"].apply(remove_after_for)
    df["ingredient"] = df["ingredient"].apply(remove_after_about)

    df["ingredient"] = df["ingredient"].str.replace(
        regex_patterns["plus_more"], "", regex=True
    )
    df["ingredient"] = df["ingredient"].replace(
        regex_patterns["if_condition"], "", regex=True
    )
    df["ingredient"] = (
        df["ingredient"].apply(lambda x: re.sub(r"  ", " ", x)).str.strip()
    )

    return df


def create_instruction_map(descriptions):
    return {inst: idx for idx, inst in enumerate(descriptions["Description"])}


def find_instruction_id(ingredient, instruction_to_id):
    for inst, idx in instruction_to_id.items():
        if inst in ingredient:
            return idx
    return None


def clean_descriptions(df, descriptions, outputdir):
    print("Cleaning descriptions...")
    instruction_to_id = create_instruction_map(descriptions)
    df["description_ID"] = df["ingredient"].apply(
        lambda x: find_instruction_id(x, instruction_to_id)
    )
    df = df.merge(
        descriptions,
        left_on="description_ID",
        right_index=True,
        how="left",
        suffixes=("", "_desc"),
    )
    mask = df["Description"].notnull()
    df.loc[mask, "ingredient"] = df.loc[mask].apply(
        lambda row: re.sub(re.escape(row["Description"]), "", row["ingredient"]), axis=1
    )
    df["ingredient"] = df["ingredient"].str.strip(" ,.()")
    df["Description"] = df["Description"].fillna("")
    descriptions.to_csv(os.path.join(outputdir, "descriptions.csv"), index=False)
    return df


def clean_measurements(df, measurements):
    print("Cleaning measurement units...")
    unit_to_index = {unit: index for index, unit in enumerate(measurements)}
    df["measurement"] = df["measurement"].map(unit_to_index)
    return df


def refactor_ingredients(recipes, ingredients, n):
    print("Refactoring ingredients iteratively...")
    ingredients = ingredients.dropna(subset=["ingredient"])
    for i in range(1, n + 1):
        print(f"Iteration {i}...")
        popular_ingredients_trie = Trie()
        ingredients_to_insert = ingredients.loc[
            ingredients["ingredientcount"] > i, "ingredient"
        ]
        for idx, ingredient in ingredients_to_insert.items():
            if len(ingredient) > 3:
                popular_ingredients_trie.insert(ingredient, idx)
        mask = ingredients["ingredientcount"] <= i
        ingredients.loc[mask, "ID_replace"] = ingredients.loc[mask, "ingredient"].apply(
            lambda x: popular_ingredients_trie.search_longest_substring(x)
        )
        id_map = ingredients.set_index("ID")["ID_replace"].dropna().to_dict()
        recipes["ID"] = recipes["ID"].map(id_map).fillna(recipes["ID"])
        ingredients = ingredients[ingredients["ID_replace"].isnull()]
    ingredients["IDnew"] = np.arange(len(ingredients))
    id_map2 = ingredients.set_index("ID")["IDnew"].to_dict()
    recipes["ingredient"] = recipes["ID"].map(id_map2).fillna(recipes["ID"])
    ingredients["ID"] = ingredients["IDnew"]
    ingredients.drop(
        [
            "ID_replace",
            "IDnew",
        ],
        axis=1,
        inplace=True,
    )
    return recipes, ingredients


def clean_data(df, ingredients, outputdir):
    print("Cleaning data...")
    df = df[df["ingredient"].notnull()]
    df = preclean(df)
    df = clean_descriptions(df, descriptions, outputdir)
    df = clean_measurements(df, measurement_units)
    df.drop(["description_ID_desc"], axis=1, inplace=True)
    df.drop(["Description"], axis=1, inplace=True)
    unique_ingredients = pd.DataFrame({"ingredient": df["ingredient"].unique()})
    unique_ingredients["ingredientcount"] = 0
    ingredients = (
        pd.concat([ingredients, unique_ingredients])
        .drop_duplicates(subset=["ingredient"])
        .reset_index(drop=True)
    )
    ingredients["ID"] = ingredients.index
    df = pd.merge(df, ingredients, on="ingredient", how="left")
    ingredient_counts = df["ingredient"].value_counts()
    ingredients["ingredientcount"] += (
        ingredients["ingredient"].map(ingredient_counts).fillna(0)
    )
    df.drop(["ingredientcount"], axis=1, inplace=True)
    return df, ingredients


def recount_IDs(df, id_map):
    next_id = 0
    print("Recounting IDs...")
    for old_id in df["recipe_id"][df["recipe_id"].notna()].unique():
        if old_id not in id_map:
            id_map[old_id] = next_id
            next_id += 1
    df["recipe_id"] = df["recipe_id"].map(id_map)
    return df, id_map


def fill_ids(df, outputdir):
    print("Filling IDs...")
    recipe_id_df = df[["recipe_id", "recipe_name", "descriptions"]].dropna()
    recipe_id_df["recipe_id"] = recipe_id_df["recipe_id"].astype(int)
    recipe_id_df = recipe_id_df.drop_duplicates(subset=["recipe_id"])
    recipe_id_df.to_csv(os.path.join(outputdir, "recipe_id.csv"), index=False)
    df.drop(["recipe_name", "descriptions"], axis=1, inplace=True)
    return df, recipe_id_df
