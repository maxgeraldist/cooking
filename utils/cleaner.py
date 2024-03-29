import pandas as pd
import re
import numpy as np
from .refactor import Trie
from .descriptions import *
from .measurement_units import measurement_units
from .useless_words import useless_words
import os


def remove_after_for(s):
    if not s.endswith(":"):
        idx = s.find("for")
        if idx != -1:
            return s[:idx]
    return s


def remove_after_or(s):
    idx = s.find(" or more")
    if idx != -1:
        return s[:idx]
    return s


def remove_before_semicolon_in_middle(s):
    if s.endswith(":"):
        return s
    else:
        idmx = s.find(";")
        if idmx != -1:
            return s[idmx + 1 :]
    return s


def remove_after_and(s):
    idx = s.find(" and ")
    if idx != -1:
        return s[:idx]
    return s


def remove_after_about(s):
    if not s.startswith("about"):
        idx = s.find("about ")
        if idx != -1:
            return s[:idx]
        return s
    else:
        s = s[5:]
    return s


def preclean(df):
    print("Precleaning...")
    df = df[df["ingredient"].notna()]
    for word in useless_words:
        df["ingredient"] = df["ingredient"].str.replace(word, "", regex=True)
    df["ingredient"] = df["ingredient"].str.replace("[,.()]", "", regex=True)
    df["ingredient"] = df["ingredient"].str.lower()
    df["ingredient"] = df["ingredient"].str.strip(" ,.()")
    df = df[~df["ingredient"].str.startswith("*")]
    df["ingredient"] = df["ingredient"].str.strip(" ,.()")
    maskpreclean = df["ingredient"].str.startswith("of ")
    df.loc[maskpreclean, "ingredient"] = df.loc[maskpreclean, "ingredient"].str[3:]
    df["ingredient"] = df["ingredient"].str.strip(" ,.()")
    df = df[~df["ingredient"].str.startswith("ingredient info")]
    maskequipment = df["ingredient"].str.startswith("special equipment")
    df.loc[maskequipment, "descriptions"] = df[maskequipment].apply(
        lambda x: str(x["ingredient"]) + "\n\n" + str(x["descriptions"]), axis=1
    )
    df.loc[maskequipment, "ingredient"] = ""
    maskarticle = df["ingredient"].str.startswith("a ")
    df.loc[maskarticle, "ingredient"] = df.loc[maskarticle, "ingredient"].str[2:]
    df["ingredient"].str.replace("plus more.*", "", regex=True, inplace=True)
    df["ingredient"] = df["ingredient"].str.strip(" ,.()")
    df[df["ingredient"].str.startswith("plus ")] = df["ingredient"].str[5:]
    df["ingredient"] = df["ingredient"].apply(remove_after_and)
    df[df["ingredient"].str.startswith("and ")] = df["ingredient"].str[4:]
    df["ingredient"] = df["ingredient"].str.strip(" ,.()")
    df["ingredient"] = df["ingredient"].apply(remove_before_semicolon_in_middle)
    df["ingredient"] = df["ingredient"].apply(remove_after_or)
    df["ingredient"].replace(" ly ", "", regex=True, inplace=True)
    df[df["ingredient"].str.startswith("or ")] = df["ingredient"].str[3:]
    df["ingredient"] = df["ingredient"].str.strip(" ,.()")
    df["ingredient"] = df["ingredient"].str.replace(r"\s*if.*", "", regex=True)
    df["ingredient"] = df["ingredient"].apply(lambda x: re.sub(r" such as .*", "", x))
    df["ingredient"] = df["ingredient"].apply(remove_after_for)
    df["ingredient"] = df["ingredient"].str.strip(" ,.()")
    df["ingredient"] = df["ingredient"].apply(remove_after_about)
    df["ingredient"] = df["ingredient"].str.strip(" ,.()")
    df["ingredient"] = df["ingredient"].apply(lambda x: re.sub(r"  ", " ", x))
    df["ingredient"] = df["ingredient"].apply(lambda x: re.sub(r"  ", " ", x))
    df["ingredient"] = df["ingredient"].str.strip(" ,.()")
    df["ingredient"] = df["ingredient"].dropna()
    return df


def clean_descriptions(df, descriptions, outputdir):
    print("Cleaning descriptions...")
    df["instruction_ID"] = df["ingredient"].apply(
        lambda x: (
            descriptions[descriptions["Instruction"].apply(lambda y: y in x)].index[0]
            if any(descriptions["Instruction"].apply(lambda y: y in x))
            else None
        )
    )
    df = df.merge(descriptions, left_on="instruction_ID", right_index=True, how="left")
    mask = df["Instruction"].notnull()
    df.loc[mask, "ingredient"] = df.loc[mask].apply(
        lambda x: re.sub(re.escape(x["Instruction"]), "", x["ingredient"]), axis=1
    )
    df["ingredient"] = df["ingredient"].str.strip(" ,.()")
    descriptions.to_csv(os.path.join(outputdir, "descriptions.csv"), index=False)
    return df


def clean_measurements(df, measurements):
    print("Cleaning measurement units...")
    unit_to_index = {unit: index for index, unit in enumerate(measurements)}
    df["measurement"] = df["measurement"].map(unit_to_index)
    return df


def replace_id1(id, id_map):
    id_replace = id_map.get(id)
    if pd.notnull(id_replace) and isinstance(id_replace, (int, float)):
        return id_replace
    else:
        return id


def replace_ids2(id, id_map2):
    id_replace = id_map2.get(id)
    if pd.notnull(id_replace) and isinstance(id_replace, (int, float)):
        return id_replace
    else:
        return id


def refactor_ingredients(recipes, ingredients, n):
    print("Refactoring ingredients...")
    ingredients = ingredients.dropna()
    popular_ingredients_trie = Trie()
    for index, row in ingredients.iterrows():
        if row["ingredientcount"] > n and len(row["ingredient"]) > 3:
            popular_ingredients_trie.insert(row["ingredient"], index)
    for index, row in ingredients.iterrows():
        if row["ingredientcount"] <= n:
            ingredients.at[index, "ID_replace"] = (
                popular_ingredients_trie.search_longest_substring(
                    row["ingredient"].replace("[:,.()]", "")
                )
            )
    id_map = ingredients.set_index("ID")["ID_replace"].to_dict()
    recipes["ID"] = recipes["ID"].apply(lambda x: replace_id1(x, id_map))
    ingredients = ingredients[ingredients["ID_replace"].isnull()]
    ingredients = ingredients.assign(IDnew=range(len(ingredients)))
    id_map2 = ingredients.set_index("ID")["IDnew"].to_dict()
    recipes["ID"] = recipes["ID"].apply(lambda x: replace_ids2(x, id_map2))
    recipes["ingredient"] = recipes["ID"]
    recipes = recipes.drop(["ID"], axis=1)
    ingredients["ID"] = ingredients["IDnew"]
    ingredients = ingredients.drop(["ID_replace"], axis=1)
    ingredients = ingredients.drop(["IDnew"], axis=1)
    return recipes, ingredients


def clean_data(df, ingredients, outputdir):
    print("Cleaning data...")
    df = clean_descriptions(df, descriptions, outputdir)
    df = clean_measurements(df, measurement_units)
    df.drop(["Index"], axis=1, inplace=True)
    df.drop(["Instruction"], axis=1, inplace=True)
    df = df[df["ingredient"].notnull()]
    unique_ingredients = pd.DataFrame({"ingredient": df["ingredient"].unique()})
    unique_ingredients["ingredientcount"] = 0
    ingredients = (
        pd.concat([ingredients, unique_ingredients])
        .drop_duplicates(subset=["ingredient"])
        .reset_index(drop=True)
    )
    ingredients["ID"] = ingredients.index
    df = pd.merge(df, ingredients, on="ingredient", how="left")
    # Deleteing recipe_ids only to then assign them again
    mask = df["recipe_id"] != df["recipe_id"].shift()
    df.loc[~mask, ["recipe_id", "recipe_name", "descriptions"]] = np.nan
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


# create a dataframe that links recipe_name and descriptions to recipe_id, remove name and descriptions from df, save the new df to a csv, then ffill the recipe_id columns
def fill_ids(df, outputdir):
    print("Filling IDs...")
    recipe_id_df = df[["recipe_id", "recipe_name", "descriptions"]].dropna()
    recipe_id_df.to_csv(os.path.join(outputdir, "recipe_id.csv"), index=False)
    df.drop(["recipe_name", "descriptions"], axis=1, inplace=True)
    df["recipe_id"] = df["recipe_id"].fillna(method="ffill")
    return df
