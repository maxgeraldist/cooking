import pyodbc


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
        cursor.execute(
            "INSERT INTO Recipes (recipe_id, ingredient, amount, measurement, instruction_ID) values (?,?,?,?,?)",
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
        "CREATE TABLE ingredients (ingredient_id int, ingredient_name varchar(2550))"
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
        "CREATE TABLE descriptions (description varchar(225), description_id int)"
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
        "CREATE TABLE measurement_units (unit_id int, unit_name varchar(255))"
    )
    for index, unit in enumerate(measurement_units):
        cursor.execute(
            "INSERT INTO measurement_units (unit_id, unit_name) values (?,?)",
            index,
            unit,
        )
        conn.commit()


def into_sql_instructions(filepath, user, pw):
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
    cursor.execute(
        "LOAD DATA LOCAL INFILE '" + filepath + "' INTO TABLE instructions "
        "FIELDS TERMINATED BY ',' "
        "OPTIONALLY ENCLOSED BY '\"' "
        "LINES TERMINATED BY '\\n' "
        "IGNORE 1 LINES;"
    )
    conn.commit()
