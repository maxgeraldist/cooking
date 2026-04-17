# Recipe JSON to MySQL Database Converter

This project processes recipe data stored in JSON files, cleans and normalizes ingredient information, and exports the structured data into a MySQL relational database. It includes a companion script for querying recipes by available ingredients.

## Overview

The pipeline performs the following tasks:

- Parses JSON files containing recipe titles, instructions, and ingredient lists.
- Cleans ingredient strings by removing punctuation, common stop words, and measurement indicators.
- Extracts structured fields: `amount`, `measurement unit`, and `ingredient name`.
- Maps preparation instructions (e.g., "chopped", "diced") to a predefined list of descriptions.
- Refactors ingredient names to merge similar entries using a trie-based substring matching algorithm.
- Assigns unique IDs to ingredients, recipes, and measurement units.
- Inserts data into a MySQL database with proper foreign key relationships.
- Provides a query script to search for recipes that match user-supplied ingredients.

## Features

- **Robust Ingredient Parsing**: Handles fractions (e.g., `1/2`, `⅔`), number words (`one`, `two`), ranges (`2 to 3`), and common phrases (`to taste`).
- **Predefined Descriptions**: Automatically identifies and tags preparation steps like `chopped`, `diced`, `sliced`.
- **Ingredient Refactoring**: Merges less frequent ingredients into more common parent ingredients (e.g., `"fresh basil leaves"` → `"basil"`) using configurable aggressiveness.
- **Large Dataset Support**: Processes data in chunks and writes CSV intermediates to avoid memory overload.
- **MySQL Integration**: Creates normalized tables (`recipes`, `ingredients`, `recipe_details`, `descriptions`, `measurement_units`) with foreign keys.
- **Interactive Query Tool**: Search for recipes by entering ingredients you have on hand; returns recipes sorted by match count and displays full details.

## Requirements

- Python 3.7+
- Required Python packages (install via `pip`):
  - `pandas`
  - `numpy`
  - `sqlalchemy`
  - `pymysql` (or `mysql-connector-python` / `pyodbc` for query script)
- MySQL server running locally (or modify connection string accordingly)
- Input JSON files in the format described below.

## Input JSON Format

The parser expects JSON files where each key is a recipe ID (string) and the value is an object with:

```
{
  "12345": {
    "title": "Recipe Name",
    "instructions": "Step-by-step directions...",
    "ingredients": [
      "2 cups flour",
      "1/2 tsp salt",
      "one large egg, beaten"
    ]
  }
}
```

Place your JSON files inside the `Input_files/` directory. Multiple files are supported.

## Setup

1. **Clone or download** this repository.

2. **Install dependencies**:
   ```
   pip install pandas numpy sqlalchemy pymysql
   ```

3. **Prepare input files**: Place your JSON recipe files in `Input_files/`.

4. **Configure MySQL**: Ensure a database named `Cooking` exists on your MySQL server. The scripts will create/replace tables automatically.

5. **Credentials**: On first run, the main script will prompt for MySQL username and password and offer to save them in `login.txt` for future use.

## Usage

### Step 1: Process JSON and Populate Database

Run the main conversion script from the project root:

```
python cooking/json_to_sql.py
```

You will be asked to set the refactoring aggressiveness (integer, recommended range 1–10). Higher values merge more ingredients, reducing uniqueness but improving grouping.

The script will:

- Process all JSON files in `Input_files/`.
- Write intermediate CSV files to `Output_files/` (helpful for debugging).
- Insert data into MySQL tables.

### Step 2: Query Recipes by Ingredients

After the database is populated, use the query tool:

```
python cooking/requests.py
```

Enter ingredients one by one, typing `STOP` when finished. The script will:

- Find recipes that contain all or most of the specified ingredients.
- Display a numbered list of matching recipes.
- Upon selection, show the full ingredient list (with amounts and units) and cooking instructions.

## Database Schema

### Table: `recipes`
| Column         | Type          | Description                     |
|----------------|---------------|---------------------------------|
| `recipe_id`    | INT           | Primary key                     |
| `recipe_name`  | VARCHAR(2550) | Name of the recipe              |
| `descriptions` | TEXT          | Cooking instructions            |

### Table: `ingredients`
| Column       | Type         | Description                |
|--------------|--------------|----------------------------|
| `ID`         | INT          | Primary key                |
| `ingredient` | VARCHAR(255) | Normalized ingredient name |

### Table: `recipe_details`
| Column            | Type    | Description                                 |
|-------------------|---------|---------------------------------------------|
| `id`              | BIGINT  | Auto-increment surrogate key                |
| `recipe_id`       | INT     | Foreign key to `recipes.recipe_id`          |
| `amount`          | INT     | Numeric quantity (integer, may be fraction) |
| `measurement`     | INT     | Foreign key to `measurement_units.unit_id`  |
| `ingredient`      | INT     | Foreign key to `ingredients.ID`             |
| `description_ID`  | INT     | Foreign key to `descriptions.description_ID`|

### Table: `descriptions`
| Column            | Type         | Description                         |
|-------------------|--------------|-------------------------------------|
| `description_ID`  | INT          | Primary key                         |
| `Description`     | VARCHAR(225) | Preparation instruction (e.g., "chopped") |

### Table: `measurement_units`
| Column      | Type         | Description         |
|-------------|--------------|---------------------|
| `unit_id`   | INT          | Primary key         |
| `unit_name` | VARCHAR(255) | Unit name (e.g., "cup", "tsp") |

A special ID `9999` is used to represent missing or empty values for `measurement` and `description_ID`.

## Customization

- **Measurement Units**: Edit `utils/measurement_units.py` to add or remove supported units.
- **Descriptions**: Modify the list in `utils/descriptions.py` to change which preparation terms are extracted.
- **Stop Words**: Adjust `utils/useless_words.py` to improve ingredient name cleaning.

## Notes

- The main script writes large CSV files to `Output_files/`. Ensure sufficient disk space.
- Processing time depends on input size; a few thousand recipes can be processed in seconds.
- The query tool uses `pymysql` by default. If you prefer another driver, modify the connection strings accordingly.

## License

This project is provided as-is for educational and personal use. Adapt and extend as needed.
