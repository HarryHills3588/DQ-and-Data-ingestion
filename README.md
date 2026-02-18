# Ingestion
```SQL
CREATE SCHEMA sources
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

```SQL
GRANT USAGE ON SCHEMA sources TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA sources TO anon, authenticated, service_role;
GRANT ALL ON ALL ROUTINES IN SCHEMA sources TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA sources TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA sources GRANT ALL ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA sources GRANT ALL ON ROUTINES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA sources GRANT ALL ON SEQUENCES TO anon, authenticated, service_role;
```

