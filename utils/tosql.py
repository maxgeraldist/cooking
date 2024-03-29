import pyodbc


def into_sql_recipedetails(df, user, pw):
    conn = pyodbc.connect(
        "DRIVER={MySQL};SERVER=localhost;DATABASE=Cooking;USER="
        + user
        + ";PASSWORD="
        + pw
        + ";Trusted_Connection=yes"
    )
    cursor = conn.cursor()
    cursor.execute(
        "CREATE OR REPLACE TABLE recipe_details (id bigint auto_increment primary key, recipe_id int, ingredient int, amount int, measurement int, description_ID int)"
    )
    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO recipe_details (recipe_id, ingredient, amount, measurement, description_ID) values (?,?,?,?,?)",
            int(row["recipe_id"]),
            int(row["ingredient"]),
            int(row["amount"]),
            int(row["measurement"]),
            int(row["instruction_ID"]),
        )
    conn.commit()


def into_sql_ingredients(filepath, user, pw):
    conn = pyodbc.connect(
        "DRIVER={MySQL};SERVER=localhost;DATABASE=Cooking;USER="
        + user
        + ";PASSWORD="
        + pw
        + ";Trusted_Connection=yes"
    )
    cursor = conn.cursor()
    cursor.execute(
        "CREATE OR REPLACE TABLE ingredients (ingredient_id int PRIMARY KEY, ingredient_name varchar(2550))"
    )
    cursor.execute(
        "LOAD DATA LOCAL INFILE '" + filepath + "' INTO TABLE ingredients "
        "FIELDS TERMINATED BY ',' "
        "OPTIONALLY ENCLOSED BY '\"' "
        "LINES TERMINATED BY '\\n' "
        "IGNORE 1 LINES;"
    )
    conn.commit()


def into_sql_descriptions(filepath, user, pw):
    conn = pyodbc.connect(
        "DRIVER={MySQL};SERVER=localhost;DATABASE=Cooking;USER="
        + user
        + ";PASSWORD="
        + pw
        + ";Trusted_Connection=yes"
    )
    cursor = conn.cursor()
    cursor.execute(
        "CREATE OR REPLACE TABLE descriptions (description varchar(225), description_id int PRIMARY KEY)"
    )
    cursor.execute(
        "LOAD DATA LOCAL INFILE '" + filepath + "' INTO TABLE descriptions "
        "FIELDS TERMINATED BY ',' "
        "OPTIONALLY ENCLOSED BY '\"' "
        "LINES TERMINATED BY '\\n' "
        "IGNORE 1 LINES;"
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
        "CREATE OR REPLACE TABLE measurement_units (unit_id int PRIMARY KEY, unit_name varchar(255))"
    )
    for index, unit in enumerate(measurement_units):
        cursor.execute(
            "INSERT INTO measurement_units (unit_id, unit_name) values (?,?)",
            index,
            unit,
        )
        conn.commit()


def into_sql_recipes(filepath, user, pw):
    conn = pyodbc.connect(
        "DRIVER={MySQL};SERVER=localhost;DATABASE=Cooking;USER="
        + user
        + ";PASSWORD="
        + pw
        + ";Trusted_Connection=yes"
    )
    cursor = conn.cursor()
    cursor.execute(
        "CREATE OR REPLACE TABLE recipes (recipe_id int PRIMARY KEY, recipe_name varchar(2550), recipe_descriptions TEXT)"
    )
    cursor.execute(
        "LOAD DATA LOCAL INFILE '" + filepath + "' INTO TABLE recipes "
        "FIELDS TERMINATED BY ',' "
        "OPTIONALLY ENCLOSED BY '\"' "
        "LINES TERMINATED BY '\\n' "
        "IGNORE 1 LINES;"
    )
    conn.commit()
