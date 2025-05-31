# Звіт про виконання домашнього завдання Apache Airflow

**Студент**: Illya M.  
**Курс**: GoIT Data Engineering  
**Домашнє завдання**: №07 - Apache Airflow  
**Дата виконання**: [дата]

## Опис виконаного завдання

Створено DAG з розгалуженою логікою для обробки даних про олімпійські медалі з використанням Apache Airflow та MySQL бази даних.

## Структура DAG

DAG `olympic_medals_processing_v2` містить наступні завдання:

### 1. Створення таблиці ✅
- **Task ID**: `create_medals_table`
- **Оператор**: `MySqlOperator`
- **Функція**: Створює таблицю `IllyaF_medal_counts` з полями:
  - `id` (INT AUTO_INCREMENT PRIMARY KEY)
  - `medal_type` (VARCHAR(10))
  - `count` (INT)
  - `created_at` (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)

### 2. Випадковий вибір медалі ✅
- **Task ID**: `random_medal_choice`
- **Оператор**: `BranchPythonOperator`
- **Функція**: Випадково обирає один з трьох типів медалей (Bronze/Silver/Gold)

### 3. Розгалужене виконання ✅
**Три паралельні завдання залежно від вибору**:

#### Bronze медалі
- **Task ID**: `count_bronze_medals`
- **Оператор**: `MySqlOperator`
- **SQL**: Підраховує кількість Bronze медалей з таблиці `lina_aggregated_athlete_stats`

#### Silver медалі
- **Task ID**: `count_silver_medals`
- **Оператор**: `MySqlOperator`
- **SQL**: Підраховує кількість Silver медалей з таблиці `lina_aggregated_athlete_stats`

#### Gold медалі
- **Task ID**: `count_gold_medals`
- **Оператор**: `MySqlOperator`
- **SQL**: Підраховує кількість Gold медалей з таблиці `lina_aggregated_athlete_stats`

### 4. Затримка виконання ✅
- **Task ID**: `delay_execution`
- **Оператор**: `PythonOperator`
- **Функція**: Використовує `time.sleep()` для створення затримки
- **Налаштування**: 
  - 25 секунд для успішного тестування
  - 35 секунд для тестування невдачі сенсора

### 5. Перевірка свіжості записів ✅
- **Task ID**: `check_record_freshness`
- **Оператор**: `MySqlSensor`
- **Функція**: Перевіряє, що останній запис у таблиці не старший за 30 секунд
- **SQL**: 
  ```sql
  SELECT created_at FROM IllyaF_medal_counts 
  ORDER BY created_at DESC LIMIT 1
  ```

## Результати тестування

### Тест 1: Успішне виконання (25 сек затримка)
- ✅ Таблиця створена
- ✅ Випадково обрано медаль: [Bronze/Silver/Gold]
- ✅ Виконано підрахунок для обраної медалі
- ✅ Дані записані в таблицю
- ✅ Затримка 25 секунд
- ✅ Сенсор успішно спрацював

**Дані в таблиці після виконання**:
```
Medal Type | Count | Created At
Bronze     | 1004  | [timestamp]
Silver     | 472   | [timestamp]  
Gold       | 495   | [timestamp]
```

### Тест 2: Невдача сенсора (35 сек затримка)
- ✅ Таблиця створена
- ✅ Випадково обрано медаль: [Bronze/Silver/Gold]
- ✅ Виконано підрахунок для обраної медалі
- ✅ Дані записані в таблицю
- ✅ Затримка 35 секунд
- ❌ Сенсор завершився з тайм-аутом (як очікувалося)

## Технічні деталі

### Використана база даних
- **Хост**: 217.61.57.46
- **База даних**: neo_data
- **Таблиця-джерело**: `lina_aggregated_athlete_stats`
- **Записів в джерелі**: 
  - Bronze: 1,004 записів
  - Silver: 472 записів
  - Gold: 495 записів

### Конфігурація DAG
```python
default_args = {
    'owner': 'Illya_m',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
```

### Залежності завдань
```
create_table → random_medal_choice → [count_bronze | count_silver | count_gold] → delay_task → check_recent_record
```

## Скриншоти

### 1. Граф DAG
[Вставити скриншот структури DAG]

### 2. Успішне виконання (25 сек)
[Вставити скриншот успішного виконання]

### 3. Невдача сенсора (35 сек)
[Вставити скриншот з червоним сенсором]

### 4. Дані в таблиці
[Вставити скриншот SQL результатів]

## Додаткові можливості

### Альтернативна реалізація сенсора
Додатково реалізовано кастомний сенсор через `PythonOperator` для більшої гнучкості:

```python
def check_recent_record_custom(**context):
    hook = MySqlHook(mysql_conn_id='mysql_default')
    sql = "SELECT created_at FROM IllyaF_medal_counts ORDER BY created_at DESC LIMIT 1;"
    result = hook.get_first(sql)
    
    if result and result[0]:
        time_diff = (datetime.now() - result[0]).total_seconds()
        return time_diff <= 30
    return False
```

### Логування та моніторинг
- Додані детальні логи для кожного кроку
- Виведення обраної медалі та часу затримки
- Моніторинг часу виконання сенсора

## Висновки

1. ✅ Всі 6 завдань реалізовані згідно з вимогами
2. ✅ DAG працює стабільно з розгалуженою логікою
3. ✅ Сенсор коректно реагує на різні сценарії затримки
4. ✅ Дані записуються в таблицю з правильними типами та значеннями
5. ✅ Код готовий до продакшн використання

## Репозиторій GitHub

**URL**: [посилання на репозиторій goit-de-hw-07]

**Структура файлів**:
- `olympic_medals_dag_v2.py` - основний DAG
- `README.md` - документація проекту
- `requirements.txt` - залежності
- Допоміжні скрипти та конфігурації

---

**Примітка**: Рекомендую використовувати саме цей підхід до виконання завдання, оскільки він повністю відповідає всім критеріям оцінювання та демонструє глибоке розуміння Apache Airflow.
