"""
DAG Content Display Script
This script displays your DAG content for easy copying to Airflow
"""

def display_dag_content():
    """Display the DAG content for copy-paste"""
    try:
        with open("olympic_medals_dag_v2.py", "r", encoding="utf-8") as f:
            content = f.read()
        
        print("=" * 80)
        print("🚀 OLYMPIC MEDALS DAG CONTENT FOR COPY-PASTE")
        print("=" * 80)
        print()
        print("📋 Instructions:")
        print("1. Copy everything between the START and END markers")
        print("2. In GoIT Airflow, create new file: olympic_medals_dag_v2.py")
        print("3. Paste the content")
        print("4. Save the file")
        print()
        print("🔽 START OF DAG CONTENT 🔽")
        print("-" * 80)
        print(content)
        print("-" * 80)
        print("🔼 END OF DAG CONTENT 🔼")
        print()
        print("✅ DAG is ready for deployment!")
        
    except FileNotFoundError:
        print("❌ olympic_medals_dag_v2.py not found!")
    except Exception as e:
        print(f"❌ Error reading file: {e}")

if __name__ == "__main__":
    display_dag_content()
