import pyodbc
import pandas as pd


def into_sql_recipes(df, user, pw):
    conn = pyodbc.connect(
        "DRIVER={MySQL};SERVER=localhost;DATABASE=Cooking;USER="
        + user
        + ";PASSWORD="
        + pw
        + ";Trusted_Connection=yes"
    )
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE Recipes (recipe_id int, ingredient int, amount int, measurement int, instruction_ID int)"
    )
    for index, row in df.iterrows():
        measurement = row["measurement"]
        if pd.isna(measurement):
            measurement = 9999
        else:
            measurement = int(measurement)
        instruction_ID = row["instruction_ID"]
        if pd.isna(instruction_ID):
            instruction_ID = 9999
        else:
            instruction_ID = int(instruction_ID)
        cursor.execute(
            "INSERT INTO Recipes (recipe_id, ingredient, amount, measurement, instruction_ID) values (?,?,?,?,?)",
            int(row["recipe_id"]),
            int(row["ingredient"]),
            int(row["amount"]),
            measurement,
            instruction_ID,
        )
        conn.commit()


def into_sql_ingredients(df, user, pw):
    conn = pyodbc.connect(
        "DRIVER={MySQL};SERVER=localhost;DATABASE=Cooking;USER="
        + user
        + ";PASSWORD="
        + pw
        + ";Trusted_Connection=yes"
    )
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE ingredients (ingredient_id int, ingredient_name varchar(2550))"
    )
    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO ingredients (ingredient_id, ingredient_name) values (?,?)",
            row["ID"],
            row["ingredient"],
        )
        conn.commit()


def into_sql_descriptions(df, user, pw):
    conn = pyodbc.connect(
        "DRIVER={MySQL};SERVER=localhost;DATABASE=Cooking;USER="
        + user
        + ";PASSWORD="
        + pw
        + ";Trusted_Connection=yes"
    )
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE descriptions (description_id int, description varchar(255))"
    )
    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO descriptions (description_id, description) values (?,?)",
            row["Index"],
            row["Instruction"],
        )
        conn.commit()


def into_sql_measurement_units(measurement_units, user, pw):
    conn = pyodbc.connect(
        "DRIVER={MySQL};SERVER=localhost;DATABASE=Cooking;USER="
        + user
        + ";PASSWORD="
        + pw
        + ";Trusted_Connection=yes"
    )
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE measurement_units (unit_id int, unit_name varchar(255))"
    )
    for index, unit in enumerate(measurement_units):
        cursor.execute(
            "INSERT INTO measurement_units (unit_id, unit_name) values (?,?)",
            index,
            unit,
        )
        conn.commit()


def into_sql_instructions(df, user, pw):
    conn = pyodbc.connect(
        "DRIVER={MySQL};SERVER=localhost;DATABASE=Cooking;USER="
        + user
        + ";PASSWORD="
        + pw
        + ";Trusted_Connection=yes"
    )
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE instructions (recipe_id int, recipe_name varchar(2550), recipe_instructions TEXT)"
    )
    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO instructions (recipe_id, recipe_name, recipe_instructions) values (?,?,?)",
            row["recipe_id"],
            row["recipe_name"],
            row["instructions"],
        )
        conn.commit()
