# Overview

This is the individual assignment 2 project. This is just the code section. All code lies in the src folder, with the main pipeline inside scripts, where you can find some data exploration as well as the main pipeline in **ingestion.py**. This is a hybrid pipeline which uses a python script to call procedural calls and logs all the calls that are being made to the database. The stored functions that are called via the python script are provided here, just make sure to store them as functions in your supabase account, as well as creating a .env file including the SUPABASE_URL and SUPABASE_KEY. All of the set up will be described below

## Process
1. Data ingestion via Python to Supabase
2. Initial SQL cleaning 
3. Python Data Quality Processing / Verification
4. 3 NF to Supabase DB

**Steps:**
- Load raw CSV 
- Remove exact duplicates
- Standardize Country & Department 
- Normalize dates 
- Fix Performance Rating
- Null out invalid Age & Salary
- Impute missing salaries
- Python-based deduplication and duplicate handling
- CREATE TABLE employees_clean 
- Insert into normalized schema

# Respository Structure
```
├── data
│   ├── cleaned_data
│   │   ├── countries_rows.csv
│   │   ├── departments_rows.csv
│   │   ├── employee_performance_rows.csv
│   │   └── employees_rows.csv
│   └── employee_data.csv
├── README.md
└── src
    ├── scripts
    │   ├── ingestion.py
    │   └── read.ipynb
    └── utils
        ├── db_connection.py
        └── transformations.py
```
- Data: holds all data, pre-ingestion and after cleaning and 3NF form is in the cleaned_data folder
- src: holds all source code
    - scripts holds main pipelines and data exploration
    - Utils holds python transformations and database clients

# Instructions
## Supabase 
Firstly, you will need a supabase project and creating a database inside the project. Name this database whatever you want, just make sure that it is a supabase instance (the free tier will be fine)

### Setup
Now that supabase account and basic database have been set up we are going to go over the important steps, go to SQL Editor Tab and run the following:
```SQL
CREATE SCHEMA sources
```
```SQL
GRANT CREATE ON DATABASE postgres TO service_role;
GRANT CREATE ON SCHEMA sources TO service_role;
GRANT USAGE ON SCHEMA sources TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA sources TO anon, authenticated, service_role;
GRANT ALL ON ALL ROUTINES IN SCHEMA sources TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA sources TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA sources GRANT ALL ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA sources GRANT ALL ON ROUTINES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA sources GRANT ALL ON SEQUENCES TO anon, authenticated, service_role;
```
```SQL
CREATE TABLE sources.employees (
    employee_id INT,
    name TEXT,
    age INT,
    department TEXT,
    date_of_joining DATE,
    years_of_experience INT,
    country TEXT,
    salary FLOAT,
    performance_rating TEXT,
    total_sales INT,
    support_rating INT
);
```
These two statements will create the schema for ingestion and the first ingestion table and manage all permissions for the API Accordingly.

Now, make sure to expose the sources schema to the public.
1. go to Project Settings tab in the left panel
2. Select Data API on the left menu
3. Select settings in main menu
4. make sure that sources schema is exposed

This completes our setup for the database. Next we look at stored functions

### Stored Functions
1. Go to the Database tab on the left panel
2. Select functions on the left panel
3. Under the sources schema, create the following functions using the GUI:

clean_employee_data() --> void
```SQL
BEGIN
    UPDATE sources.employees
    SET age = NULL
    WHERE age::NUMERIC < 0 OR age::NUMERIC > 120;

    UPDATE sources.employees
    SET salary = NULL
    WHERE salary IS NOT NULL AND salary::NUMERIC < 0;
END;
```

create_clean_emp_tbl() --> void
```SQL
BEGIN
    DROP TABLE IF EXISTS employees_clean CASCADE;

    CREATE TABLE sources.employees_clean AS
    SELECT
        employee_id::INT,
        name,
        age::INT,
        department,
        date_of_joining::DATE,
        years_of_experience::INT,
        country,
        salary::NUMERIC(12,2),
        performance_rating::INT,
        total_sales::NUMERIC(14,2),
        support_rating::INT
    FROM sources.employees;
END;
```
department_correction() --> void
```SQL
DECLARE
    updated_count integer;
BEGIN
    UPDATE sources.employees
    SET department = CASE TRIM(LOWER(department))
        WHEN 'markting'    THEN 'Marketing'
        WHEN 'mkt'         THEN 'Marketing'
        WHEN 'mktg'        THEN 'Marketing'
        WHEN 'marketingg'  THEN 'Marketing'
        WHEN 'sls'         THEN 'Sales'
        WHEN 'sale'        THEN 'Sales'
        WHEN 'sales'       THEN 'Sales'
        WHEN 'saless'      THEN 'Sales'
        WHEN 'slaes'       THEN 'Sales'
        WHEN 'supp'        THEN 'Support'
        WHEN 'supprt'      THEN 'Support'
        WHEN 'supportt'    THEN 'Support'
        WHEN 'suport'      THEN 'Support'
        ELSE INITCAP(TRIM(department))
    END
    WHERE TRIM(LOWER(department)) IN ('markting', 'mkt', 'mktg', 'marketingg', 'sls', 'sale', 'saless', 'slaes', 'supp', 'supprt', 'supportt', 'suport');
END;
```
impute_median_salary() --> void
```SQL

BEGIN
    UPDATE sources.employees r
    SET salary = sub.dept_median
    FROM (
        SELECT
            department,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary::NUMERIC) AS dept_median
        FROM sources.employees
        WHERE salary IS NOT NULL
        GROUP BY department
    ) sub
    WHERE r.department = sub.department
      AND r.salary IS NULL;
END;
```

norm_form() --> void
```SQL
BEGIN
    CREATE TABLE IF NOT EXISTS staging.countries (
        country_id   SERIAL PRIMARY KEY,
        country_name VARCHAR(45) NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS staging.departments (
        department_id   SERIAL PRIMARY KEY,
        department_name VARCHAR(100) NOT NULL UNIQUE
    );

    INSERT INTO staging.countries (country_name)
    SELECT DISTINCT country FROM sources.employees_clean WHERE country IS NOT NULL;

    INSERT INTO staging.departments (department_name)
    SELECT DISTINCT department FROM sources.employees_clean WHERE department IS NOT NULL;

    CREATE TABLE IF NOT EXISTS staging.employees (
        employee_id         INT PRIMARY KEY,
        name                VARCHAR(100),
        age                 INT CHECK (age > 0),
        department_id       INT REFERENCES staging.departments(department_id),
        date_of_joining     DATE,
        years_of_experience INT,
        country_id          INT REFERENCES staging.countries(country_id),
        salary              NUMERIC(10,2) CHECK (salary > 0),
        performance_rating  INT CHECK (performance_rating BETWEEN 1 AND 5)
    );

    INSERT INTO staging.employees
    SELECT
        ec.employee_id,
        ec.name,
        ec.age,
        d.department_id,
        ec.date_of_joining,
        ec.years_of_experience,
        c.country_id,
        ec.salary,
        ec.performance_rating
    FROM sources.employees_clean ec
    JOIN staging.departments d ON d.department_name = ec.department
    JOIN staging.countries  c ON c.country_name = ec.country;

    CREATE TABLE IF NOT EXISTS staging.employee_performance (
        employee_id    INT PRIMARY KEY REFERENCES staging.employees(employee_id),
        total_sales    NUMERIC(10,2) DEFAULT 0,
        support_rating INT
    );

    INSERT INTO staging.employee_performance
    SELECT employee_id, total_sales, support_rating
    FROM sources.employees_clean
    WHERE total_sales > 0 OR support_rating > 0;
END;
```

remove_duplicates() --> void
```SQL
BEGIN
    CREATE TEMPORARY TABLE employees_temp AS
    SELECT DISTINCT * FROM sources.employees;

    TRUNCATE TABLE sources.employees;

    INSERT INTO sources.employees
    SELECT * FROM employees_temp;

    DROP TABLE employees_temp;
END;
```

staging_init() --> void
```SQL
BEGIN
  CREATE SCHEMA IF NOT EXISTS staging;
END
```

standardize_country() --> void
```SQL
BEGIN
UPDATE sources.employees 
SET country = LOWER(country)
WHERE TRUE;
END;
```

standardize_joining_dates() --> void
```SQL
BEGIN
    UPDATE sources.employees
    SET date_of_joining = TO_CHAR(
        CASE
            -- YYYY-MM-DD  e.g. 2015-01-01
            WHEN date_of_joining ~ '^\d{4}-\d{2}-\d{2}$'
                THEN TO_DATE(date_of_joining, 'YYYY-MM-DD')
            -- YYYY/MM/DD  e.g. 2013/01/01
            WHEN date_of_joining ~ '^\d{4}/\d{2}/\d{2}$'
                THEN TO_DATE(date_of_joining, 'YYYY/MM/DD')
            -- MM/DD/YYYY  e.g. 01/01/2017
            WHEN date_of_joining ~ '^\d{2}/\d{2}/\d{4}$'
                THEN TO_DATE(date_of_joining, 'MM/DD/YYYY')
            -- MM-DD-YYYY  e.g. 01-01-2018
            WHEN date_of_joining ~ '^\d{2}-\d{2}-\d{4}$'
                THEN TO_DATE(date_of_joining, 'MM-DD-YYYY')
            ELSE NULL
        END,
    'YYYY-MM-DD')
    WHERE date_of_joining IS NOT NULL 
      AND date_of_joining !~ '^\d{4}-\d{2}-\d{2}$';
END;
```

standardize_performance() --> void
```SQL
BEGIN
    UPDATE sources.employees
    SET performance_rating = CASE TRIM(LOWER(performance_rating))
        WHEN 'low performer'   THEN '1'
        WHEN 'medium low'      THEN '2'
        WHEN 'medium'          THEN '3'
        WHEN 'medium high'     THEN '4'
        WHEN 'high performer'  THEN '5'
        ELSE
            CASE
                WHEN performance_rating ~ '^[1-5]$' THEN performance_rating
                ELSE NULL
            END
    END
    WHERE TRIM(LOWER(performance_rating)) IN ('low performer', 'medium low', 'medium', 'medium high', 'high performer');
END;
```

Now, for the last one go to the SQL editor and run:
```SQL
CREATE OR REPLACE FUNCTION sources.find_duplicate_employees()
RETURNS TABLE (
    employee_id INT,
    name TEXT,
    age INT,
    department TEXT,
    date_of_joining DATE,
    years_of_experience INT,
    country TEXT,
    salary NUMERIC(12,2),
    performance_rating INT,
    total_sales NUMERIC(14,2),
    support_rating INT
)
LANGUAGE plpgsql
AS $$
SELECT
    employee_id::INT,
    name,
    age::INT,
    department,
    date_of_joining::DATE,
    years_of_experience::INT,
    country,
    salary::NUMERIC(12,2),
    performance_rating::INT,
    total_sales::NUMERIC(14,2),
    support_rating::INT
FROM sources.employees_clean
WHERE employee_id IN (
    SELECT employee_id
    FROM sources.employees_clean
    GROUP BY employee_id
    HAVING COUNT(*) > 1
)
ORDER BY employee_id DESC;
$$;
```

## Python
Under the directory structure, in the main folder create a .env file with the following two enviroment variables
- SUPABASE_URL=[your database url]
- SUPABASE_KEY=[your database key]

### dependencies
Install the following dependencies
- supabase, pandas, numpy, python-dotenv
    ```
    pip install supabase pandas numpy python-dotenv
    ```

Now, make sure that the employee_data.csv file is located under data.

The project should be good to run.

# Running the Project
From here, now just run the Ingestion Script ingestion.py and it should be able to run and you should see the results populate in the staging and sources schemas.