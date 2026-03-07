import pandas as pd 
import numpy as np
import os 
import json

from pathlib import Path
from src.utils.db_connection import DBConnection
from src.utils.transformations import handle_duplicates
from supabase import Client 

if __name__ == "__main__":
    # connection to db
    db_connection = DBConnection.get_client()

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

    print("Success")
    print()
    print("2. Standardizing Countries and departments")

    std_countries = (db_connection
        .schema('sources')
        .rpc("standardize_country", {})
        .execute()
    )

    std_count_dpt = (db_connection
        .schema('sources')
        .rpc("department_correction", {})
        .execute())
    
    print("Success")
    print()

    print("4. Standardizing Performance Rate")
    std_performance = (db_connection
        .schema("sources")
        .rpc("standardize_performance",{})
        .execute()
    )

    print("Success")
    print()
    print("5. Fixing Invalid Age and Salary")
    clean_emp_data = (db_connection
        .schema('sources')
        .rpc("clean_employee_data", {})
        .execute())

    print("Success")
    print()
    print("6. Imputing Missing Data and finalizing dealing with duplicates")
    impute = (db_connection
        .schema('sources')
        .rpc("impute_median_salary", {})
        .execute())
    
    duplicates = (db_connection
                  .schema("sources")
                  .rpc("remove_duplicates",{})
                  .execute()
    )

    print("Success")
    print()
    print("CREATING NEW CLEAN_EMPLOYEES TABLE")
    clean_creation_response = (db_connection
        .schema('sources')
        .rpc('create_clean_emp_tbl',{})
        .execute()
    )
    
    db_connection = DBConnection.get_client()

    handle_duplicates(db_connection=db_connection)

    print('CREATING STAGING SCHEMA IF NOT EXISTS')
    staging_init = (db_connection
        .schema('sources')
        .rpc('staging_init',{})
        .execute()
    )

    print("CREATING 3NF TABLES AND POPULATING THEM")
    norm_form = (db_connection
        .schema('sources')
        .rpc('norm_form', {})
        .execute()
    )

    print("FINISHED")
