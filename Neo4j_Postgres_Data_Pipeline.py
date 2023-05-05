# Import required libraries
from neo4j import GraphDatabase
import pandas as pd
import psycopg2

# Define Neo4j connection details
neo4j_uri = "bolt://localhost:7687"
neo4j_user = "ubo"
neo4j_password = "admin@123"

# Define Postgres connection details
connection_string = 'postgresql://postgres:ubo123@34.173.130.172:5432/postgres'

# Define Neo4j query to extract data
neo4j_query = '''
    MATCH (c:Customer)
    RETURN c.customer_id, c.service_id,c.subscription_id,c.start_date_subscription,c.end_date_subscription,c.price_of_subscription
    '''
#Create driver object
driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

# Define function to extract data from Neo4j and return a Pandas DataFrame
def extract_data():
    with driver.session() as session:
    
        result = session.run(neo4j_query)
        records = []
        for record in result:
          records.append(dict(record))
        df = pd.DataFrame(records)
        return df


# Define function to transform data
def transform_data(df):
    # Convert date fields to datetime objects
    df["c.start_date_subscription"] = pd.to_datetime(df["c.start_date_subscription"])
    df["c.end_date_subscription"] = pd.to_datetime(df["c.end_date_subscription"])
    df["c.service_id"] =df["c.service_id"].astype(int)
    
    # Remove null values
    df.dropna(inplace = True)
    # Rename columns
    df = df.rename(columns = {"c.customer_id" : "customer_id" , "c.service_id" : "service_id",\
                                "c.start_date_subscription":"start_date","c.end_date_subscription" :"end_date","c.price_of_subscription":" price", "c.subscription_id" : "subscription_id"})
    
    #Drop Duplicates
    df.drop_duplicates(inplace= True)
    return df

# Define function to load data into Postgres
def load_data(df):
    # Connect to Postgres
    conn = psycopg2.connect(connection_string)
    # Create table if it doesn't exist
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS telecom_data (
            customer_id INTEGER,
            subscription_id INTEGER,
            service_id INTEGER,
            start_date DATE,
            end_date DATE,
            price FLOAT
        )
        """)
    # Insert data into table
        insert_query = "INSERT INTO telecom_data (customer_id,service_id,subscription_id,start_date,end_date,price) VALUES (%s,%s,%s,%s,%s,%s)"
        data = [tuple(row) for row in df.values]
        cursor.executemany(insert_query, data)
        conn.commit()
        conn.close()

def read_data():
      """
    Returns a cleaned Dataframe.
            
            Parameters:
                    connection string to the database
            Returns:
                    Returns a Dataframe
       """
     #Create a connection to the database using pyscop
      conn = psycopg2.connect(connection_string)
      #Create the query to be executed
      query = "SELECT * FROM  telecom_data"
      # Execute query,create a dataframe and display the five records.
      queried_frame = pd.read_sql(query,conn)
      print(queried_frame.head())

# Define main function
def main():
    # Extract data from Neo4j
    extracted_data = extract_data()
    # Transform data using Pandas
    transformed_data = transform_data(extracted_data)
    # Load data into Postgres
    load_data(transformed_data)
    # Read data from Database to Test
    read_data()
    
    

# Call main function
if __name__ == "__main__":
    main()