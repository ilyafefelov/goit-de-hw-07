"""
Airflow DAG Deployment Helper
=============================

This script helps deploy the Olympic Medals DAG to your Airflow instance.

Instructions:
1. Make sure Airflow is running on localhost:8080
2. Copy olympic_medals_dag_v2.py to your Airflow dags folder
3. The DAG should appear in Airflow UI within 30 seconds

For Docker Airflow setup, the typical command would be:
docker cp olympic_medals_dag_v2.py <airflow_container>:/opt/airflow/dags/

Testing Scenarios:
- For successful sensor test: Keep delay_seconds = 25 in the DAG
- For failed sensor test: Change delay_seconds = 35 in the DAG

DAG Structure:
create_table → random_medal_choice → [count_bronze|count_silver|count_gold] → delay_task → [check_recent_record, check_recent_record_python]
"""

import os
import shutil

def find_airflow_dags_folder():
    """Try to find common Airflow dags folder locations"""
    common_paths = [
        os.path.expanduser("~/airflow/dags"),
        "./dags",
        "../dags", 
        "C:/airflow/dags",
        "/opt/airflow/dags"
    ]
    
    for path in common_paths:
        if os.path.exists(path):
            return path
    
    return None

def deploy_dag():
    """Deploy DAG to Airflow dags folder if found"""
    dags_folder = find_airflow_dags_folder()
    
    if dags_folder:
        try:
            source = "olympic_medals_dag_v2.py"
            destination = os.path.join(dags_folder, "olympic_medals_dag_v2.py")
            shutil.copy2(source, destination)
            print(f"✅ DAG deployed to: {destination}")
            print("🚀 Check Airflow UI at http://localhost:8080 in 30 seconds")
            return True
        except Exception as e:
            print(f"❌ Failed to copy DAG: {e}")
            return False
    else:
        print("⚠️  Airflow dags folder not found automatically")
        print("📋 Manual deployment required:")
        print("   1. Locate your Airflow dags folder")
        print("   2. Copy olympic_medals_dag_v2.py to that folder")
        print("   3. Check Airflow UI at http://localhost:8080")
        return False

if __name__ == "__main__":
    print("🚀 Olympic Medals DAG - Airflow Deployment Helper")
    print("=" * 50)
    
    if os.path.exists("olympic_medals_dag_v2.py"):
        print("✅ DAG file found: olympic_medals_dag_v2.py")
        deploy_dag()
    else:
        print("❌ DAG file not found: olympic_medals_dag_v2.py")
        print("   Make sure you're in the correct directory")
