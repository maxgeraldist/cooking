import pandas as pd 
import numpy as np


def drop_duplicates(df):
    for i in range(len(df)):
        if df['recipe_id'][i] != None:
            
                    
