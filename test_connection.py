"""
Швидка перевірка підключення до MySQL та структури таблиць
"""
import mysql.connector
from dotenv import load_dotenv
import os

def test_connection():
    """Тестування підключення до бази даних"""
    load_dotenv()
    
    try:
        # Підключення до бази даних
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        
        cursor = connection.cursor()
        
        print("✅ Підключення успішне!")
          # Перевірка існування таблиці lina_aggregated_athlete_stats
        cursor.execute("""
            SELECT COUNT(*) as table_exists 
            FROM information_schema.tables 
            WHERE table_schema = 'neo_data' 
            AND table_name = 'lina_aggregated_athlete_stats'
        """)
        
        exists = cursor.fetchone()[0]
        if exists:
            print("✅ Таблиця lina_aggregated_athlete_stats існує")
            
            # Підрахунок медалей для тестування
            medal_counts = {}
            for medal in ['Bronze', 'Silver', 'Gold']:
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM lina_aggregated_athlete_stats 
                    WHERE medal = '{medal}'
                """)
                count = cursor.fetchone()[0]
                medal_counts[medal] = count
                print(f"  {medal}: {count} записів")
        else:
            print("❌ Таблиця lina_aggregated_athlete_stats не знайдена")
        
        # Перевірка існування таблиці medal_counts
        cursor.execute("""
            SELECT COUNT(*) as table_exists 
            FROM information_schema.tables 
            WHERE table_schema = 'neo_data' 
            AND table_name = 'IllyaF_medal_counts'
        """)
        
        exists = cursor.fetchone()[0]
        if exists:
            print("✅ Таблиця IllyaF_medal_counts вже існує")
            
            # Показати останні записи
            cursor.execute("""
                SELECT id, medal_type, count, created_at 
                FROM IllyaF_medal_counts 
                ORDER BY created_at DESC 
                LIMIT 5
            """)
            
            records = cursor.fetchall()
            if records:
                print("Останні записи:")
                for record in records:
                    print(f"  ID: {record[0]}, Medal: {record[1]}, Count: {record[2]}, Time: {record[3]}")
            else:
                print("  Таблиця порожня")
        else:
            print("ℹ️ Таблиця IllyaF_medal_counts буде створена DAG")
        
    except mysql.connector.Error as error:
        print(f"❌ Помилка підключення: {error}")
    
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("🔐 З'єднання закрито")

if __name__ == "__main__":
    test_connection()
