import pandas as pd 
import numpy as np
import os 
import json

from pathlib import Path
from src.utils.db_connection import DBConnection
from supabase import Client 

if __name__ == "__main__":
    # connection to db
    db_connection = DBConnection.client

    # read file
    data_dir = Path(os.getcwd()).joinpath('data','employee_data.csv')
    df = pd.read_csv(data_dir)

    column_mapping = {
        'Employee Id': 'employee_id',
        'Name': 'name',
        'Age': 'age',
        'Department': 'department',
        'Date of Joining': 'date_of_joining',
       'Years of Experience': 'years_of_experience',
       'Country': 'country',
        'Salary': 'salary',
        'Performance Rating': 'performance_rating',
       'Total Sales': 'total_sales',
       'Support Rating': 'support_rating'
    }

    df.rename(columns=column_mapping, inplace=True)
    json_data = df.to_json(orient='records')
    json_data = json.loads(json_data)

    print(df.columns)
    
    response = (db_connection
                .schema('sources')
                .table('employees')
                .insert(json_data)
                .execute()
    )