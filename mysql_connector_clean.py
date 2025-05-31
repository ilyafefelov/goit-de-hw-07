"""
Simple MySQL connector for Olympic dataset
Essential connector class for homework submission
"""
import mysql.connector
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

class MySQLConnector:
    """MySQL connector class for Olympic dataset operations"""
    
    def __init__(self):
        """Initialize connector with environment variables"""
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
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection closed")
    
    def execute_query(self, query):
        """Execute a query and return results as DataFrame"""
        if not self.connection or not self.connection.is_connected():
            print("Not connected to database")
            return None
        
        try:
            return pd.read_sql(query, self.connection)
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

if __name__ == "__main__":
    # Quick test of the connector
    db = MySQLConnector()
    if db.connect():
        # Test query on the source table used in DAG
        result = db.execute_query("SELECT COUNT(*) as total FROM aggregated_athlete_results")
        if result is not None:
            print(f"Source table has {result.iloc[0]['total']} records")
        db.disconnect()
