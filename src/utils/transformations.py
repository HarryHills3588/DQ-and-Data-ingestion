import pandas as pd
from supabase import Client

def handle_duplicates(db_connection: Client):
    response = (db_connection.schema('sources').rpc('find_duplicate_employees',{}).execute())

    data = pd.DataFrame(response.data)

    uniqueIds = set(data['employee_id'])

    max_id_response = (db_connection
            .schema('sources')
            .table('employees_clean')
            .select('employee_id')
            .order('employee_id', desc=True)
            .limit(1)
            .execute()
        ).data[0]
    max_id:int = max_id_response['employee_id']

    for id in uniqueIds:
        duplicate = data[data['employee_id'] == id]

        # Case 1: Duplicate ID with different person
        # drop second occurrence and assign to new ID
        if duplicate['name'].iloc[0] != duplicate['name'].iloc[1]:
            to_remove = duplicate.iloc[1]
            to_remove_dict = to_remove.to_dict()
            
            max_id += 1
            to_remove_dict['employee_id'] = max_id
            remove_response = (db_connection
                .schema('sources')
                .table('employees_clean')
                .delete()
                .eq("employee_id", to_remove['employee_id'])
                .eq('name', to_remove['name'])
                .execute()
            )

            insert_removed = (db_connection
                .schema('sources')
                .table('employees_clean')
                .insert(to_remove_dict)
                .execute()
            )

        # Case 2: Duplicate ID with different salary
        elif duplicate['name'].iloc[0] == duplicate['name'].iloc[1]:
            employee_id = duplicate.iloc[0]['employee_id']
            to_insert = duplicate.iloc[0].to_dict()
            to_insert['salary'] = (to_insert['salary'] + duplicate.iloc[1]['salary'])/2
            
            max_id += 1
            to_insert['employee_id'] = max_id

            remove_response = (db_connection
                .schema('sources')
                .table('employees_clean')
                .delete()
                .eq('employee_id', employee_id)
                .execute()
            )
            print(remove_response)

            insert_new = (db_connection
                .schema('sources')
                .table('employees_clean')
                .insert(to_insert)
                .execute()
            )