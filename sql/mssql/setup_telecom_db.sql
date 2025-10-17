USE telecom_db;
GO

CREATE LOGIN airflow_user WITH PASSWORD = 'Airflow!123';
CREATE USER airflow_user FOR LOGIN airflow_user;
ALTER ROLE db_owner ADD MEMBER airflow_user;
GO

CREATE LOGIN spark_user WITH PASSWORD = 'Spark!123';
CREATE USER spark_user FOR LOGIN spark_user;
ALTER ROLE db_datareader ADD MEMBER spark_user;
ALTER ROLE db_datawriter ADD MEMBER spark_user;
GO

PRINT '✅ Users created successfully';
GO
