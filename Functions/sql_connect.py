import pandas as pd
import numpy as np 
import mysql.connector

# Path: main.py

mydb = mysql.connector.connect(
    host="sql7.freesqldatabase.com",
    user="sql7634649",
    passwd="yLCcKDtW53",
    database="sql7634649"
)
print(mydb)

