import pandas as pd
import re
import numpy as np
from refactor import Trie
from instructions import *


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


def remove_after_plus(s):
    idx = s.find("plus more")
    if idx != -1:
        return s[:idx]
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
    df["ingredient"] = df["ingredient"].str.replace("[,.()]", "", regex=True)
    df["ingredient"] = df["ingredient"].str.lower()
    df["ingredient"] = df["ingredient"].str.strip()
    df = df[~df["ingredient"].str.startswith("*")]
    df["ingredient"] = df["ingredient"].str.strip()
    maskpreclean = df["ingredient"].str.startswith("of ")
    df.loc[maskpreclean, "ingredient"] = df.loc[maskpreclean, "ingredient"].str[3:]
    df["ingredient"] = df["ingredient"].str.strip()
    df = df[~df["ingredient"].str.startswith("ingredient info")]
    maskequipment = df["ingredient"].str.startswith("special equipment")
    df.loc[maskequipment, "instructions"] = df[maskequipment].apply(
        lambda x: str(x["ingredient"]) + "\n\n" + str(x["instructions"]), axis=1
    )
    df.loc[maskequipment, "ingredient"] = ""
    maskarticle = df["ingredient"].str.startswith("a ")
    df.loc[maskarticle, "ingredient"] = df.loc[maskarticle, "ingredient"].str[2:]
    df["ingredient"] = df["ingredient"].apply(remove_after_plus)
    df["ingredient"] = df["ingredient"].str.strip()
    df["ingredient"] = df["ingredient"].apply(remove_after_and)
    df["ingredient"] = df["ingredient"].str.strip()
    df["ingredient"] = df["ingredient"].apply(remove_after_or)
    df["ingredient"] = df["ingredient"].str.strip()
    df["ingredient"] = df["ingredient"].str.replace(r"\s*if.*", "", regex=True)
    df["ingredient"] = df["ingredient"].apply(lambda x: re.sub(r" such as .*", "", x))
    df["ingredient"] = df["ingredient"].apply(remove_after_for)
    df["ingredient"] = df["ingredient"].str.strip()
    df["ingredient"] = df["ingredient"].apply(remove_after_about)
    df["ingredient"] = df["ingredient"].str.strip()
    df["ingredient"] = df["ingredient"].apply(lambda x: re.sub(r"  ", " ", x))
    df["ingredient"] = df["ingredient"].apply(lambda x: re.sub(r"  ", " ", x))
    df["ingredient"] = df["ingredient"].str.strip()
    df["ingredient"] = df["ingredient"].dropna()
    return df

#
# def clean_instructions(df, instructions):
#     print("Cleaning instructions...")
#     df["instruction_ID"] = df["ingredient"].apply(
#         lambda x: instructions[
#             instructions["Instruction"].apply(lambda y: y in x)
#         ].index[0]
#         if any(instructions["Instruction"].apply(lambda y: y in x))
#         else None
#     )
#     df = df.merge(instructions, left_on="instruction_ID", right_index=True, how="left")
#     mask = df["Instruction"].notnull()
#     df.loc[mask, "ingredient"] = df.loc[mask, "ingredient"].str.replace(
#         df["Instruction"], "", regex=False
#     )
#     df["ingredient"] = df["ingredient"].str.strip(" ,.()")
#     return df


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
        if row["ingredientcount"] > n:
            popular_ingredients_trie.insert(row["ingredient"], index)
    for index, row in ingredients.iterrows():
        if row["ingredientcount"] <= n:
            ingredients.at[
                index, "ID_replace"
            ] = popular_ingredients_trie.search_longest_substring(
                row["ingredient"].replace("[:,.()]", "")
            )
    id_map = ingredients.set_index("ID")["ID_replace"].to_dict()
    print(recipes["ID"][:10])
    recipes["ID"] = recipes["ID"].apply(lambda x: replace_id1(x, id_map))
    print(recipes["ID"][:10])
    ingredients = ingredients[ingredients["ID_replace"].isnull()]
    ingredients = ingredients.assign(IDnew=range(len(ingredients)))
    id_map2 = ingredients.set_index("ID")["IDnew"].to_dict()
    recipes["ID"] = recipes["ID"].apply(lambda x: replace_ids2(x, id_map2))
    # recipes['ingredient'] = recipes['ID']
    # recipes = recipes.drop(['ID'], axis=1)
    ingredients["ID"] = ingredients["IDnew"]
    ingredients = ingredients.drop(["ID_replace"], axis=1)
    ingredients = ingredients.drop(["IDnew"], axis=1)
    return recipes, ingredients


def clean_data(df, ingredients, n):
    print("Cleaning data...")
    # df = clean_instructions(df, instructions)
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
    # Replace ingredient IDs with the IDs of the most popular ingredients
    mask = df["recipe_id"] != df["recipe_id"].shift()
    df.loc[~mask, ["recipe_id", "recipe_name", "instructions"]] = np.nan
    ingredient_counts = df["ingredient"].value_counts()
    ingredients["ingredientcount"] += (
        ingredients["ingredient"].map(ingredient_counts).fillna(0)
    )
    # df, ingredients = refactor_ingredients(df, ingredients, n)
    df.drop(["ingredientcount"], axis=1, inplace=True)
    return df, ingredients


def recount_IDs(df, id_map):
    next_id = 0
    print("Recounting IDs...")
    for old_id in df["recipe_id"].unique():
        if old_id not in id_map:
            id_map[old_id] = next_id
            next_id += 1
    df["recipe_id"] = df["recipe_id"].map(id_map)
    return df, id_map
