"""
Switch DAG between success (25s) and failure (35s) testing modes
"""
import os
import re

def switch_delay_mode(dag_file="olympic_medals_dag_v2.py", mode="success"):
    """
    Switch between success and failure testing modes
    
    Args:
        mode: "success" (25s delay) or "failure" (35s delay)
    """
    
    if not os.path.exists(dag_file):
        print(f"❌ DAG file not found: {dag_file}")
        return False
    
    # Read current file
    with open(dag_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Determine target delay
    if mode == "success":
        new_delay = 25
        description = "успішний тест (менше 30 секунд)"
    elif mode == "failure":
        new_delay = 35
        description = "тест провалу (більше 30 секунд)"
    else:
        print("❌ Invalid mode. Use 'success' or 'failure'")
        return False
    
    # Replace delay_seconds value
    pattern = r'delay_seconds = \d+'
    new_line = f'delay_seconds = {new_delay}'
    
    if re.search(pattern, content):
        new_content = re.sub(pattern, new_line, content)
        
        # Write back to file
        with open(dag_file, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        print(f"✅ Switched to {mode} mode:")
        print(f"   Delay: {new_delay} seconds")
        print(f"   Expected: {description}")
        
        return True
    else:
        print("❌ Could not find delay_seconds in file")
        return False

def main():
    print("🚀 Olympic Medals DAG - Testing Mode Switcher")
    print("=" * 50)
    
    print("Available modes:")
    print("  1. success - 25 second delay (sensor should pass)")
    print("  2. failure - 35 second delay (sensor should fail)")
    print()
    
    mode = input("Enter mode (success/failure): ").strip().lower()
    
    if mode in ['success', 'failure']:
        if switch_delay_mode(mode=mode):
            print("\n📋 Next steps:")
            print("1. Copy updated DAG to Airflow:")
            print("   docker cp olympic_medals_dag_v2.py hardcore_jennings:/opt/bitnami/airflow/dags/")
            print("2. Wait 30 seconds for Airflow to detect changes")
            print("3. Trigger DAG in Airflow UI")
            print("4. Observe sensor behavior")
    else:
        print("❌ Invalid input. Please enter 'success' or 'failure'")

if __name__ == "__main__":
    main()
