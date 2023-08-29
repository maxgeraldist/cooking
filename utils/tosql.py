import pyodbc
import pandas as pd


def into_sql_recipes(df,user,pw):
    conn = pyodbc.connect(
        "DRIVER={MySQL ODBC 8.1 Unicode Driver};SERVER=localhost;DATABASE=local_recipes;USER=" + user + ";PASSWORD=" + pw + ";Trusted_Connection=yes"
    )
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE Recipes (recipe_id int, ingredient int, amount int, measurement int, instruction_ID int)"
    )
    for index,row in df.iterrows():
        measurement = row["measurement"]
        if  not pd.isna(measurement):
            measurement = 9999
        else:
            measurement = int(measurement)
        instruction_ID = row["instruction_ID"]
        if pd.isna(instruction_ID):
            # For example, you can set it to a default value or skip this row
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




def into_sql_ingredients(df,user,pw):
    conn = pyodbc.connect(
        "DRIVER={MySQL ODBC 8.1 Unicode Driver};SERVER=localhost;DATABASE=local_recipes;USER=" + user + ";PASSWORD=" + pw + ";Trusted_Connection=yes"
    )
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE ingredients (ingredient_id int, ingredient_name varchar(255))"
    )
    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO ingredients (ingredient_id, ingredient_name) values (?,?)",
            row["ID"],
            row["ingredient"],
        )
        conn.commit()



def into_sql_instructions(df,user,pw):
    conn = pyodbc.connect(
        "DRIVER={MySQL ODBC 8.1 Unicode Driver};SERVER=localhost;DATABASE=local_recipes;USER=" + user + ";PASSWORD=" + pw + ";Trusted_Connection=yes"
    )
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE instructions (instruction_id int, instruction varchar(255))"
    )
    for index,row in df.iterrows():
        cursor.execute(
            "INSERT INTO instructions (instruction_id, instruction) values (?,?)",
            row["Index"],
            row["Instruction"],
        )
        conn.commit()


def into_sql_measurement_units(measurement_units, user, pw):
    conn = pyodbc.connect(
        "DRIVER={MySQL ODBC 8.1 Unicode Driver};SERVER=localhost;DATABASE=local_recipes;USER=" + user + ";PASSWORD=" + pw + ";Trusted_Connection=yes"
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
