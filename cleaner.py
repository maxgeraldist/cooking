import pandas as pd
import os
import numpy as np
import time

start_time = time.time()
m=0
def clean_instructions(row, df, instructions):
   # Move the instructions to a separate table
    for i in range(len(instructions)):
        if instructions['Instruction'][i] in row['Ingredient']:
            row['Instruction'] = instructions['ID'][i]
            row['Ingredient'] = row['Ingredient'].replace(instructions['Instruction'][i], '')
            break

    return row
def clean_ingredients(row, ingredients):
    # replace all the ingredients with their ID and remove the rest of the string
    for i in range(len(ingredients)):
        if ingredients['Ingredient'][i] in row['Ingredient']:
            row['Ingredient'] = ingredients['ID'][i]
            m+=1
            if m%1000==0:
                print('ingredients processed: ', m)
            break
    return row
instructions = pd.DataFrame({'Instruction': ['minced', 'chopped', 'diced', 'sliced', 'grated', 'peeled', 'crushed', 'melted', 'mashed']})
instructions['ID'] = instructions.index

def clean_data(df):
    # apply the cleaning functions to the dataframe
    df = df.apply(clean_instructions, axis=1, args=(df, instructions))
    ingredients = pd.DataFrame({'Ingredient': df['Ingredient'].unique()})
    ingredients['ID'] = ingredients.index
    df = df.apply(clean_ingredients, axis=1, args=(ingredients,))
    df = df.dropna()
    df = df[df['Ingredient'] != '']
    return df



input_dir = 'Recipes'
output_dir = 'Cleaned_Recipes'
input_files = [os.path.join(input_dir, name) for name in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, name))]
# Read all files and concatenate them into one dataframe
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
    df = df[df['Ingredient'].notnull()]
    df['Ingredient'] = df['Ingredient'].strip(' ')
    df['Ingredient'] = df['Ingredient'].str.lower()
    df['Ingredient'] = df['Ingredient'].str.replace(',', '')
    df['Ingredient'] = df['Ingredient'].str.replace('.', '')
    df['Ingredient'] = df['Ingredient'].str.replace('(', '')
    df['Ingredient'] = df['Ingredient'].str.replace(')', '')
    clean_data(df)
    output_file = os.path.join(output_dir, os.path.basename(input_file))
    if format == 'xlsx':
        df.to_excel(output_file, index=False)
    else:
        df.to_csv(output_file, index=False)
end_time = time.time()
print('Time taken to clean data: ', end_time - start_time)