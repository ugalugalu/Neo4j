This is a Data Pipeline that extracts data from a Neo4J database, transforms and cleans it before loading into Postgres Sql database in Google cloud.

The Neo4j Database was installed locally on my machine, please see the screen shot below.

![image](https://user-images.githubusercontent.com/54645939/236464792-06995d99-113b-43cc-b4ad-d6b2a6a6f8c2.png)

Data Extraction: This is achieved by running a cql on neo4j database and saving the results on a Data frame.

Transformatopn and cleaning: The date columns are transformed to a date object using pd.to_date function. Null and Duplicate values were dropped.

Loading: Loading is achieved by using pyscopg2 driver. A table is created on postgre database with the following schema

            customer_id  as INTEGER,
            
            subscription_id as INTEGER,
            
            service_id as INTEGER,
            
            start_date as  DATE,
            
            end_date  as DATE,
            
            price as FLOAT
            


The read function tests that data has been loaded successfully to SQL database

below is a screen shot of the subsequent pipeline after running it.

![image](https://user-images.githubusercontent.com/54645939/236466203-05ef4bae-bced-4c56-ad5d-958c3239afc9.png)
