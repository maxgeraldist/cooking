"""
Functions to insert data into MySQL database.
"""

import time
import pandas as pd
from sqlalchemy import Integer, String, Text


def into_sql_recipedetails(df: pd.DataFrame, engine):
    """
    Inserting recipe details dataframe into the database using .to_sql method of sqlalchemy engine.
    """
    print("Inserting recipe details...")
    time1 = time.time()
    with engine.connect() as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE recipe_details (
                id bigint auto_increment primary key, 
                recipe_id int(11), 
                amount int, 
                measurement int, 
                ingredient int(11), 
                description_ID int(11),
                FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id),
                FOREIGN KEY (measurement) REFERENCES measurement_units(unit_id),
                FOREIGN KEY (ingredient) REFERENCES ingredients(ID),
                FOREIGN KEY (description_ID) REFERENCES descriptions(description_ID)
            );
            """
        )
        for n in range(0, len(df), 100000):
            df1 = df[n : n + 100000]
            df1.to_sql(
                name="recipe_details",
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                dtype={
                    "recipe_id": Integer(),
                    "amount": Integer(),
                    "measurement": Integer(),
                    "ingredient": Integer(),
                    "instruction_ID": Integer(),
                },
            )
    print(f"Time taken: {time.time() - time1}")


def into_sql_ingredients(df: pd.DataFrame, engine):
    """
    Inserting ingredients dataframe into the database using .to_sql method of sqlalchemy engine.
    """
    print("Inserting ingredients...")
    time1 = time.time()
    with engine.connect() as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE ingredients (
                ID INT PRIMARY KEY, 
                ingredient VARCHAR(255)
            );
            """
        )
        df = df.drop(columns=["ingredientcount"])
        df.to_sql(
            name="ingredients",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            dtype={"ID": Integer(), "ingredient": String(255)},
        )
    print(f"Time taken: {time.time() - time1}")


def into_sql_descriptions(descriptions: pd.DataFrame, engine):
    """
    Inserting descriptions dataframe into the database using .to_sql method of sqlalchemy engine.
    """
    print("Inserting descriptions...")
    time1 = time.time()
    placeholder = pd.DataFrame([{"description_ID": 9999, "Description": ""}])
    descriptions = pd.concat([descriptions, placeholder], axis=0)
    with engine.connect() as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE descriptions (
                description varchar(225), 
                description_ID int PRIMARY KEY
            );
            """
        )
        descriptions.to_sql(
            name="descriptions",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            dtype={"Description": String(225), "description_ID": Integer()},
        )
    print(f"Time taken: {time.time() - time1}")


def into_sql_measurement_units(measurement_units: list, engine):
    """
    Inserting measurement units list into the database using SQL INSERT INTO query execution
    """
    print("Inserting measurement units...")
    time1 = time.time()

    # Create a dictionary of measurement units
    measurement_units_dict = {i: unit for i, unit in enumerate(measurement_units)}
    measurement_units_dict[9999] = ""
    data = list(measurement_units_dict.items())

    with engine.begin() as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE measurement_units (
                unit_id int PRIMARY KEY, 
                unit_name varchar(255)
            );
            """
        )
        insert_query = """
            INSERT INTO measurement_units (unit_id, unit_name) 
            VALUES (%s, %s)
        """
        for row in data:
            conn.execute(insert_query, row)
    print(f"Time taken: {time.time() - time1}")


def into_sql_recipes(recipes: pd.DataFrame, engine):
    """
    Inserting recipes dataframe into the database
    using .to_sql method of sqlalchemy engine.
    """
    print("Inserting recipes...")
    time1 = time.time()
    with engine.connect() as conn:
        conn.execute(
            """
            CREATE OR REPLACE TABLE recipes (
                recipe_id int PRIMARY KEY, 
                recipe_name varchar(2550), 
                descriptions TEXT
            );
            """
        )

        for n in range(0, len(recipes), 10000):
            recipes1 = recipes[n : n + 10000]
            recipes1.to_sql(
                name="recipes",
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                dtype={
                    "recipe_id": Integer(),
                    "recipe_name": String(2550),
                    "descriptions": Text(),
                },
            )
    print(f"Time taken: {time.time() - time1}")
