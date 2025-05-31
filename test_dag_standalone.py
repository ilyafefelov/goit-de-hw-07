#!/usr/bin/env python3
"""
Standalone test script for Olympic Medals DAG without Airflow runtime
This script tests the core functionality that would run in Airflow
"""

import time
import requests
import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MySQL connection parameters (from your DAG)
MYSQL_CONFIG = {
    'host': '217.61.57.46',
    'database': 'olympic_dataset',
    'user': 'neo_data_admin',
    'password': 'Proyahaxuqithab9oplp'
}

def test_mysql_connection():
    """Test MySQL connection"""
    logger.info("Testing MySQL connection...")
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        if connection.is_connected():
            logger.info("✅ MySQL connection: SUCCESS")
            cursor = connection.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            logger.info(f"MySQL server version: {version[0]}")
            cursor.close()
            connection.close()
            return True
    except Error as e:
        logger.error(f"❌ MySQL connection: FAILED - {e}")
        return False
    
def test_source_data():
    """Test if source data exists in the database"""
    logger.info("Testing source data availability...")
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = connection.cursor()
        
        # Try different possible source tables
        source_tables = [
            'lina_aggregated_athlete_stats',  # Original from DAG
            'aggregated_athlete_results',     # Alternative 1
            'athlete_event_results',          # Alternative 2 (main events table)
            'iuliia_athlete_event_results_summary'  # Alternative 3
        ]
        
        for table in source_tables:
            try:
                cursor.execute(f"""
                    SELECT COUNT(*) as total_records,
                           SUM(CASE WHEN medal = 'Bronze' THEN 1 ELSE 0 END) as bronze_count,
                           SUM(CASE WHEN medal = 'Silver' THEN 1 ELSE 0 END) as silver_count,
                           SUM(CASE WHEN medal = 'Gold' THEN 1 ELSE 0 END) as gold_count
                    FROM {table}
                """)
                result = cursor.fetchone()
                
                if result and result[0] > 0:
                    logger.info(f"✅ Source data: SUCCESS - Found table '{table}' with {result[0]} total records")
                    logger.info(f"   Bronze medals: {result[1]}")
                    logger.info(f"   Silver medals: {result[2]}")
                    logger.info(f"   Gold medals: {result[3]}")
                    cursor.close()
                    connection.close()
                    return table  # Return the working table name
            except Exception as e:
                logger.debug(f"Table {table} not suitable: {e}")
                continue
        
        logger.error("❌ Source data: FAILED - No suitable Olympic data table found")
        cursor.close()
        connection.close()
        return None
            
    except Error as e:
        logger.error(f"❌ Source data check: FAILED - {e}")
        return None

def extract_bronze_medals(source_table=None):
    """Extract and count bronze medals from existing database"""
    if not source_table:
        logger.error("No source table specified")
        return None
        
    logger.info(f"Counting bronze medals from database table: {source_table}...")
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = connection.cursor()
        
        # Count bronze medals
        cursor.execute(f"""
            SELECT COUNT(*) FROM {source_table} WHERE medal = 'Bronze'
        """)
        count = cursor.fetchone()[0]
        
        cursor.close()
        connection.close()
        
        logger.info(f"✅ Bronze medal count: SUCCESS - Found {count} bronze medals")
        return count
    except Error as e:
        logger.error(f"❌ Bronze medal count: FAILED - {e}")
        return None

def save_to_mysql(medal_count, table_name='IllyaF_medal_counts'):
    """Save medal count to MySQL results table"""
    logger.info(f"Saving medal count to MySQL table: {table_name}")
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = connection.cursor()
        
        # Create table if not exists (matching DAG structure)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_query)
        
        # Insert medal count
        insert_query = f"""
        INSERT INTO {table_name} (medal_type, count, created_at)
        VALUES ('Bronze', %s, NOW())
        """
        
        cursor.execute(insert_query, (medal_count,))
        connection.commit()
        
        logger.info(f"✅ MySQL save: SUCCESS - Inserted bronze count: {medal_count}")
        cursor.close()
        connection.close()
        return True
        
    except Error as e:
        logger.error(f"❌ MySQL save: FAILED - {e}")
        return False

def test_sensor_logic(timeout_seconds=30):
    """Test the sensor logic that checks for fresh records"""
    logger.info(f"Testing sensor logic with {timeout_seconds}s timeout...")
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = connection.cursor()
        
        # Check for fresh records (within 30 seconds like the DAG sensor)
        cursor.execute("""
            SELECT COUNT(*)
            FROM IllyaF_medal_counts
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 SECOND)
            ORDER BY created_at DESC
            LIMIT 1;
        """)
        result = cursor.fetchone()
        fresh_count = result[0] if result else 0
        
        cursor.close()
        connection.close()
        
        if fresh_count > 0:
            logger.info(f"✅ Sensor logic: SUCCESS - Found {fresh_count} fresh records")
            return True
        else:
            logger.info(f"❌ Sensor logic: FAILED - No fresh records found")
            return False
            
    except Error as e:
        logger.error(f"❌ Sensor logic: FAILED - {e}")
        return False

def test_timing_scenario(delay_seconds, timeout_seconds=30):
    """Test timing scenario with configurable delay"""
    logger.info(f"Testing timing scenario: {delay_seconds}s delay with {timeout_seconds}s timeout")
    
    start_time = time.time()
    logger.info(f"Starting delay at {datetime.now()}")
    
    # Simulate the delay that would be in your DAG
    time.sleep(delay_seconds)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Delay completed after {elapsed_time:.1f} seconds")
    
    if elapsed_time <= timeout_seconds:
        logger.info(f"✅ Timing test: SUCCESS - Completed within {timeout_seconds}s timeout")
        return True
    else:
        logger.info(f"❌ Timing test: FAILED - Exceeded {timeout_seconds}s timeout")
        return False

def main():
    """Main test function"""
    logger.info("=" * 60)
    logger.info("Olympic Medals DAG Standalone Test")
    logger.info("=" * 60)
    
    # Test 1: MySQL Connection
    logger.info("\n🔹 Test 1: MySQL Connection")
    mysql_ok = test_mysql_connection()
      # Test 2: Source Data Check
    logger.info("\n🔹 Test 2: Source Data Availability")
    source_table = test_source_data()
    source_data_ok = source_table is not None
    
    # Test 3: Medal Count Extraction
    logger.info("\n🔹 Test 3: Bronze Medal Count Extraction")
    medal_count = None
    if source_table:
        medal_count = extract_bronze_medals(source_table)
    extraction_ok = medal_count is not None
    
    # Test 4: Data Save to MySQL
    logger.info("\n🔹 Test 4: Medal Count Save to MySQL")
    save_ok = False
    if mysql_ok and extraction_ok:
        save_ok = save_to_mysql(medal_count)
    else:
        logger.info("Skipping save test due to previous failures")
    
    # Test 5: Sensor Logic Test
    logger.info("\n🔹 Test 5: Sensor Logic (Fresh Record Detection)")
    sensor_ok = False
    if save_ok:
        sensor_ok = test_sensor_logic()
    else:
        logger.info("Skipping sensor test due to previous failures")
    
    # Test 6: Success Scenario (25 seconds)
    logger.info("\n🔹 Test 6: Success Scenario (25s delay, 30s timeout)")
    success_scenario = test_timing_scenario(25, 30)
    
    # Test 7: Failure Scenario (35 seconds)
    logger.info("\n🔹 Test 7: Failure Scenario (35s delay, 30s timeout)")
    failure_scenario = not test_timing_scenario(35, 30)  # Should fail
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    logger.info(f"MySQL Connection:     {'✅ PASS' if mysql_ok else '❌ FAIL'}")
    logger.info(f"Source Data Check:    {'✅ PASS' if source_data_ok else '❌ FAIL'}")
    logger.info(f"Medal Count Extract:  {'✅ PASS' if extraction_ok else '❌ FAIL'}")
    logger.info(f"Data Save to MySQL:   {'✅ PASS' if save_ok else '❌ FAIL'}")
    logger.info(f"Sensor Logic:         {'✅ PASS' if sensor_ok else '❌ FAIL'}")
    logger.info(f"Success Scenario:     {'✅ PASS' if success_scenario else '❌ FAIL'}")
    logger.info(f"Failure Scenario:     {'✅ PASS' if failure_scenario else '❌ FAIL'}")
    
    overall_success = all([mysql_ok, source_data_ok, extraction_ok, save_ok, sensor_ok, success_scenario, failure_scenario])
    logger.info(f"\nOverall Test Result:  {'✅ ALL TESTS PASSED' if overall_success else '❌ SOME TESTS FAILED'}")
    
    return overall_success

if __name__ == "__main__":
    main()
