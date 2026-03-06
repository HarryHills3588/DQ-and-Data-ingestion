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
    
    try:
        response = (db_connection
                    .schema('sources')
                    .table('employees')
                    .insert(json_data)
                    .execute()
        )
    except Exception as e:
        print(e)
    else:
        print(response)

    print()
    print("Proceeding to data cleaning")
    print("1. Removing duplicates")
    duplicates = (db_connection
                  .schema("sources")
                  .rpc("remove_duplicates",{})
                  .execute()
    )

    id_collisions = (db_connection
            .schema('sources')
            .rpc("id_collisions", {})
            .execute())
    
    ## TODO: Do something with id_collisions

    print("2. Standardizing and departments")
    std_count_dpt = (db_connection
        .schema('sources')
        .rpc("department_correction", {})
        .execute())

    print("3. Standardizing Dates")
    std_dates = (db_connection
        .schema('sources')
        .rpc("standardize_joining_dates", {})
        .execute())

    print("4. Standardizing Performance Rate")
    std_performance = (db_connection
        .schema("sources")
        .rpc("standardize_performance",{})
        .execute()
    )

    print("5. Fixing Invalid Age and Salary")
    clean_emp_data = (db_connection
        .schema('sources')
        .rpc("clean_employee_data", {})
        .execute())

    print("6. Imputing Missing Data")
    impute = (db_connection
        .schema('sources')
        .rpc("impute_median_salary", {})
        .execute())
