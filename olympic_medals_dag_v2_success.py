from datetime import datetime, timedelta
import random
import time
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.sensors.mysql import MySqlSensor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.trigger_rule import TriggerRule

# Конфігурація DAG
default_args = {
    'owner': 'Illya_m',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'olympic_medals_processing_v2_success',
    default_args=default_args,
    description='Process Olympic medals data with branching logic (Success Test)',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['olympic', 'medals', 'mysql', 'v2', 'success-test']
)

# Завдання 1: Створення таблиці
create_table = MySqlOperator(
    task_id='create_medals_table',
    mysql_conn_id='mysql_default',
    sql="""
    CREATE TABLE IF NOT EXISTS Illya_F_medal_counts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal_type VARCHAR(10),
        count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag
)

# Завдання 2: Випадковий вибір медалі
def choose_random_medal(**context):
    """Випадково обирає один з трьох типів медалей"""
    medals = ['Bronze', 'Silver', 'Gold']
    chosen_medal = random.choice(medals)
    print(f"Chosen medal: {chosen_medal}")
    
    # Зберігаємо вибрану медаль в XCom для можливого використання
    context['task_instance'].xcom_push(key='chosen_medal', value=chosen_medal)
    
    # Повертаємо task_id для розгалуження
    if chosen_medal == 'Bronze':
        return 'count_bronze_medals'
    elif chosen_medal == 'Silver':
        return 'count_silver_medals'
    else:
        return 'count_gold_medals'

random_medal_choice = BranchPythonOperator(
    task_id='choose_medal_type',
    python_callable=choose_random_medal,
    dag=dag
)

# Завдання 3-4: Три завдання для підрахунку медалей

# Підрахунок Bronze медалей
count_bronze = MySqlOperator(
    task_id='count_bronze_medals',
    mysql_conn_id='mysql_default',    sql="""
    INSERT INTO IllyaF_medal_counts (medal_type, count, created_at)
    SELECT 'Bronze', COUNT(*), NOW()
    FROM lina_aggregated_athlete_stats
    WHERE medal = 'Bronze';
    """,
    dag=dag
)

# Підрахунок Silver медалей
count_silver = MySqlOperator(
    task_id='count_silver_medals',
    mysql_conn_id='mysql_default',    sql="""
    INSERT INTO IllyaF_medal_counts (medal_type, count, created_at)
    SELECT 'Silver', COUNT(*), NOW()
    FROM lina_aggregated_athlete_stats
    WHERE medal = 'Silver';
    """,
    dag=dag
)

# Підрахунок Gold медалей
count_gold = MySqlOperator(
    task_id='count_gold_medals',
    mysql_conn_id='mysql_default',    sql="""
    INSERT INTO IllyaF_medal_counts (medal_type, count, created_at)
    SELECT 'Gold', COUNT(*), NOW()
    FROM lina_aggregated_athlete_stats
    WHERE medal = 'Gold';
    """,
    dag=dag
)

# Завдання 5: Затримка виконання
def create_delay(**context):
    """Створює затримку для тестування сенсора"""
    delay_seconds = 25  # 25 секунд для успішного тесту, змініть на 35 для тесту провалу
    print(f"Starting delay for {delay_seconds} seconds...")
    time.sleep(delay_seconds)
    print("Delay completed!")

delay_task = PythonOperator(
    task_id='delay_execution',
    python_callable=create_delay,
    trigger_rule=TriggerRule.ONE_SUCCESS,  # Виконується якщо хоча б одне з попередніх завдань успішне
    dag=dag
)

# Завдання 6: Сенсор для перевірки свіжості даних
check_recent_record = MySqlSensor(
    task_id='check_record_freshness',
    mysql_conn_id='mysql_default',
    sql="""
    SELECT COUNT(*)
    FROM IllyaF_medal_counts
    WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 SECOND)
    ORDER BY created_at DESC
    LIMIT 1;
    """,
    poke_interval=10,  # Перевіряємо кожні 10 секунд
    timeout=60,        # Таймаут 60 секунд
    mode='poke',
    dag=dag
)

# Альтернативний сенсор з кастомною логікою
def check_recent_record_custom(**context):
    """Перевіряє, чи є свіжі записи в таблиці"""
    hook = MySqlHook(mysql_conn_id='mysql_default')
    
    sql = """
    SELECT created_at
    FROM IllyaF_medal_counts
    ORDER BY created_at DESC
    LIMIT 1;
    """
    
    result = hook.get_first(sql)
    if result and result[0]:
        latest_time = result[0]
        current_time = datetime.now()
        time_diff = (current_time - latest_time).total_seconds()
        
        print(f"Latest record time: {latest_time}")
        print(f"Current time: {current_time}")
        print(f"Time difference: {time_diff} seconds")
        
        if time_diff <= 30:
            print("Record is fresh (within 30 seconds)")
            return True
        else:
            print("Record is too old (more than 30 seconds)")
            return False
    else:
        print("No records found")
        return False

check_recent_record_python = PythonOperator(
    task_id='check_record_freshness_python',
    python_callable=check_recent_record_custom,
    dag=dag
)

# Налаштування залежностей
create_table >> random_medal_choice

# Розгалуження на три завдання підрахунку
random_medal_choice >> [count_bronze, count_silver, count_gold]

# Затримка виконується після будь-якого з завдань підрахунку
[count_bronze, count_silver, count_gold] >> delay_task

# Сенсори виконуються після затримки
delay_task >> [check_recent_record, check_recent_record_python]
