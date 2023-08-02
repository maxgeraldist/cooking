import pandas as pd
import os
import time

start_time = time.time()

def clean_instructions(df, instructions):
    for i in range(len(instructions)):
        mask = df['ingredient'].str.contains(instructions['Instruction'][i])
        df.loc[mask, 'Instruction'] = instructions['ID'][i]
        df.loc[mask, 'ingredient'] = df.loc[mask, 'ingredient'].str.replace(instructions['Instruction'][i], '')
    return df

def clean_ingredients(df, ingredients):
    # replace all the ingredients with their id and remove the rest of the string
    for i in range(len(ingredients)):
        mask1 = df['ingredient'].str.contains(ingredients['ingredient'][i], na=False)
        df.loc[mask1, 'ingredient'] = ingredients['ID'][i]
    return df

instructions = pd.DataFrame({'Instruction': ['minced', 'chopped', 'diced', 'sliced', 'grated', 'peeled', 'crushed', 'melted', 'mashed']})
instructions['ID'] = instructions.index
ingredients = pd.DataFrame(columns=['ID', 'ingredient'])
def clean_data(df, ingredients):
    # apply the cleaning functions to the dataframe
    df = clean_instructions(df, instructions)
    ingredients = pd.concat([ingredients['ingredient'], pd.DataFrame({'ingredient': df['ingredient'].unique()})])
    df = clean_ingredients(df, ingredients)
    df = df.dropna(subset=['ingredient'])
    df = df[df['ingredient'] != '']
    return df

input_dir = 'Recipes'
output_dir = 'Cleaned_Recipes'
input_files = [os.path.join(input_dir, name) for name in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, name))]
# Read all files and concatenate them into one dataframe
i = 0
for input_file in input_files:
    rows=[]
    if '.xlsx' in input_file:
        df = pd.read_excel(input_file, engine='openpyxl')
        format = True
    elif '.csv' in input_file:
        df = pd.read_csv(input_file)
        format = False
    else:
        continue
    df = df[df['ingredient'].notnull()]
    df['ingredient'] = df['ingredient'].str.strip(' ')
    df['ingredient'] = df['ingredient'].str.lower()
    df['ingredient'] = df['ingredient'].str.replace(',', '')
    df['ingredient'] = df['ingredient'].str.replace('.', '')
    df['ingredient'] = df['ingredient'].str.replace('(', '')
    df['ingredient'] = df['ingredient'].str.replace(')', '')
    clean_data(df, ingredients)
    output_file = os.path.join(output_dir, os.path.basename(input_file))
    if format == 'xlsx':
        df.to_excel('input_file'+str(i), index=False)
    else:
        df.to_csv('output_file' +str(i), index=False)

ingredients['ID'] = ingredients.index
if format == 'csv':
    ingredients.to_csv('ingredients.csv', index=False)
    instructions.to_csv('instructions.csv', index=False)
else:
    ingredients.to_excel('ingredients.xlsx', index=False)
    instructions.to_excel('instructions.xlsx', index=False)
end_time = time.time()
print('Time taken to clean data: ', end_time - start_time)

