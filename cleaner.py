import pandas as pd
import os
import time
import re

start_time = time.time()

def remove_after_for(s):
    if not s.endswith(':'):
        idx = s.find('for')
        if idx != -1:
            return s[:idx]
    return s
def remove_before_semicolon_in_middle(s):
    if s.endswith(':'):
        return s
    else:
        idmx = s.find(';')
        if idmx != -1:
            return s[idmx+1:]
def preclean(df):
    df = df[df['ingredient'].notna()]
    df['ingredient'] = df['ingredient'].str.lower()
    df['ingredient'] = df['ingredient'].str.strip(' ,.')
    df = df[~df['ingredient'].str.startswith('*')]
    maskpreclean = df['ingredient'].str.startswith('of ')
    df.loc[maskpreclean, 'ingredient'] = df.loc[maskpreclean, 'ingredient'].str[3:]
    df = df[~df['ingredient'].str.startswith('ingredient info')]
    df['ingredient'] = df['ingredient'].str.replace('[,.()]', '', regex=True)
    maskequipment = df['ingredient'].str.startswith('special equipment')
    df.loc[maskequipment, 'instructions'] = df[maskequipment].apply(lambda x: str(x['ingredient']) + '\n\n' + str(x['instructions']), axis=1)
    df.loc[maskequipment, 'ingredient'] = ''
    return df

def clean_instructions(df, instructions):
    df['ingredient'] = df['ingredient'].str.replace(r'\s*if.*', '', regex=True)
    df['ingredient'] = df['ingredient'].apply(lambda x: re.sub(r' or .*', '', x))
    df['ingredient'] = df['ingredient'].apply(lambda x: re.sub(r' such as .*', '', x))
    df['ingredient'] = df['ingredient'].apply(remove_after_for)
    df['Instruction'] = df['ingredient'].str.extract(f'({"|".join(instructions["Instruction"])})', expand=False)
    df = df.merge(instructions, on='Instruction', how='left')
    mask = df['Instruction'].notnull()
    df.loc[mask, 'ingredient'] = df.loc[mask, 'ingredient'].str.replace(df['Instruction'], '', regex=False)
    df.drop('Instruction', axis=1, inplace=True)
    return df

instructions = pd.DataFrame({'Instruction': ['minced', 'chopped', 'diced', 'sliced', 'grated', 'peeled', 'crushed', 
                                             'melted', 'mashed','cut','condensed','uncooked','cooked','cored','shredded',
                                             'ground','drained','pounded','rinsed','dried','dissolved','a little',
                                             'scrubbed',"thawed","quartered","halved","squeezed","trimmed","softened",
                                             "chilled","to cover","to coat","to serve","toasted","to garnish"
                                             "to finish","to decorate","to dust","to line","to glaze","to sprinkle",
                                             "to season","to roll","to grease","to crush","chilled"]})
instructions['instruction_ID'] = instructions.index
ingredients = pd.DataFrame(columns=['ID', 'ingredient'])

def clean_data(df, ingredients):
    df = clean_instructions(df, instructions)
    df = df[df['ingredient'].notnull()]
    unique_ingredients = pd.DataFrame({'ingredient': df['ingredient'].unique()})
    print(unique_ingredients)
    ingredients = pd.concat([ingredients, unique_ingredients]).drop_duplicates(subset=['ingredient']).reset_index(drop=True)
    print(ingredients)
    ingredients['ID'] = ingredients.index
    df = pd.merge(df, ingredients, on='ingredient', how='left')
    df['ingredient'] = df['ID']
    df.drop(['ID'], axis=1, inplace=True)
    return df, ingredients
input_dir = 'Recipes'
output_dir = 'Cleaned_Recipes'
input_files = [os.path.join(input_dir, name) for name in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, name))]

i = 1
for input_file in input_files:
    if '.xlsx' in input_file:
        df = pd.read_excel(input_file, engine='openpyxl')
        format = True
    elif '.csv' in input_file:
        df = pd.read_csv(input_file)
        format = False
    else:
        continue
    df = preclean(df)
    df, ingredients = clean_data(df, ingredients)
    output_file = os.path.join(output_dir, os.path.basename(input_file))
    if format:
        df.to_excel(output_dir+'/output_file' +str(i)+'.xlsx', index=False)
    else:
        df.to_csv(output_dir+'/output_file' +str(i)+'.csv', index=False)
    i += 1

if format == False:
    ingredients.to_csv('ingredients.csv', index=False)
    instructions.to_csv('instructions.csv', index=False)
else:
    ingredients.to_excel('ingredients.xlsx', index=False)
    instructions.to_excel('instructions.xlsx', index=False)

end_time = time.time()
print('Time taken to clean data: ', end_time - start_time)
