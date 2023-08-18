import pandas as pd
import time
import os

timestart = time.time()

ingredients = pd.read_csv('ingredients_refactored.csv')
input_location = 'Cleaned_Recipes'
output_location = 'Refactored_Recipes'

input_files = [os.path.join(input_location, name) for name in os.listdir(input_location) if os.path.isfile(os.path.join(input_location, name))]

def replace_ids(recipes, ingredients):
    # Create a dictionary to map old IDs to new IDs
    id_map = ingredients.set_index('ID')['ID_replace'].to_dict()
    
    # Replace old IDs with new IDs in the 'ingredients' column of the 'recipes' DataFrame
    recipes['ingredients'] = recipes['ingredients'].apply(lambda x: [id_map.get(i, i) for i in x])

for input_file in input_files:
    recipe = pd.read_csv(input_file)
    recipe = replace_ids(recipe, ingredients)
    recipe.to_csv(os.path.join(output_location, 
