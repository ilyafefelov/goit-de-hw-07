import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

class MySQLConnector:
    def __init__(self):
        self.host = os.getenv('DB_HOST')
        self.database = os.getenv('DB_NAME')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.port = int(os.getenv('DB_PORT', 3306))
        self.connection = None
        
    def connect(self):
        """Establish connection to MySQL database"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            print(f"Successfully connected to MySQL database: {self.database}")
            return True
        except mysql.connector.Error as e:
            print(f"Error connecting to MySQL: {e}")
            return False
    
    def disconnect(self):
        """Close the database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection closed")
    
    def get_tables(self):
        """Get list of all tables in the database"""
        if not self.connection or not self.connection.is_connected():
            print("No active connection")
            return []
        
        try:
            cursor = self.connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            cursor.close()
            return tables
        except mysql.connector.Error as e:
            print(f"Error fetching tables: {e}")
            return []
    
    def execute_query(self, query):
        """Execute a SQL query and return results as pandas DataFrame"""
        if not self.connection or not self.connection.is_connected():
            print("No active connection")
            return None
        
        try:
            df = pd.read_sql(query, self.connection)
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
    
    def get_table_info(self, table_name):
        """Get information about a specific table"""
        if not self.connection or not self.connection.is_connected():
            print("No active connection")
            return None
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            cursor.close()
            
            print(f"\nTable: {table_name}")
            print("-" * 50)
            for column in columns:
                print(f"Column: {column[0]}, Type: {column[1]}, Null: {column[2]}, Key: {column[3]}")
            
            return columns
        except mysql.connector.Error as e:
            print(f"Error getting table info: {e}")
            return None
    
    def get_sample_data(self, table_name, limit=5):
        """Get sample data from a table"""
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return self.execute_query(query)

def main():
    # Create database connector
    db = MySQLConnector()
    
    # Connect to database
    if not db.connect():
        return
    
    try:
        # Get list of tables
        print("Available tables:")
        tables = db.get_tables()
        for i, table in enumerate(tables, 1):
            print(f"{i}. {table}")
        
        if not tables:
            print("No tables found in the database")
            return
        
        print("\n" + "="*60)
        
        # Show information for each table
        for table in tables:
            print(f"\nAnalyzing table: {table}")
            print("="*60)
            
            # Get table structure
            db.get_table_info(table)
            
            # Get sample data
            print(f"\nSample data from {table}:")
            sample_data = db.get_sample_data(table)
            if sample_data is not None and not sample_data.empty:
                print(sample_data.to_string(index=False))
            else:
                print("No data found or unable to retrieve data")
            
            print("\n" + "-"*60)
        
        # Example: Get all data from first table
        if tables:
            first_table = tables[0]
            print(f"\nRetrieving all data from '{first_table}' table:")
            all_data = db.execute_query(f"SELECT * FROM {first_table}")
            if all_data is not None:
                print(f"Total rows: {len(all_data)}")
                print("\nFirst 10 rows:")
                print(all_data.head(10).to_string(index=False))
                
                # Save to CSV
                csv_filename = f"{first_table}_data.csv"
                all_data.to_csv(csv_filename, index=False)
                print(f"\nData exported to: {csv_filename}")
    
    finally:
        # Always close the connection
        db.disconnect()

if __name__ == "__main__":
    main()
