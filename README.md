# GoIT Data Engineering Homework 07 - Apache Airflow

## Опис завдання

Це домашнє завдання з Apache Airflow, яке демонструє створення DAG з розгалуженою логікою, роботою з MySQL базою даних та використанням сенсорів.

## Структура проекту

```
goit-de-hw-07/
├── olympic_medals_dag_v2.py        # Основний DAG (Airflow 2.x)
├── mysql_connector.py              # Скрипт для підключення до MySQL
├── requirements.txt                # Python залежності
├── .env                           # Конфігурація бази даних
├── IllyaF_athlete_enriched_avg_data.csv  # Приклад даних
└── README.md                      # Цей файл
```

## Функціонал DAG

DAG `olympic_medals_processing_v2` виконує наступні завдання:

1. **Створення таблиці** (`create_medals_table`)
   - Створює таблицю `IllyaF_medal_counts` з полями: id, medal_type, count, created_at

2. **Випадковий вибір медалі** (`random_medal_choice`)
   - Використовує `BranchPythonOperator` для випадкового вибору Bronze/Silver/Gold

3. **Розгалужене виконання** (`count_bronze`, `count_silver`, `count_gold`)
   - Залежно від вибору, виконується один з трьох завдань підрахунку медалей
   - Підраховує кількість записів у таблиці `olympic_dataset.athlete_event_results`
   - Вставляє результат у створену таблицю

4. **Затримка** (`delay_execution`)
   - Використовує `time.sleep(35)` для створення затримки
   - 35 секунд використовується для тестування сценарію невдачі сенсора

5. **Перевірка свіжості даних** (`check_record_freshness`)
   - Використовує `MySqlSensor` для перевірки, що останній запис не старший за 30 секунд
   - Включає також альтернативну реалізацію через `PythonOperator`

## Налаштування

### 1. Встановлення залежностей

```bash
pip install -r requirements.txt
```

### 2. Налаштування підключення до MySQL в Airflow

Створіть підключення `mysql_default` в Airflow Admin -> Connections:
- Connection Id: `mysql_default`
- Connection Type: `MySQL`
- Host: `217.61.57.46`
- Database: `neo_data`
- Login: `neo_data_admin`
- Password: `[ваш пароль]`

### 3. Копіювання DAG файлу

Скопіюйте `olympic_medals_dag_v2.py` до папки `dags` вашого Airflow:

```bash
cp olympic_medals_dag_v2.py $AIRFLOW_HOME/dags/
```

## Тестування

### Сценарій успішного виконання
Змініть затримку на 25 секунд у завданні `delay_execution`:
```python
time.sleep(25)  # Сенсор спрацює успішно
```

### Сценарій невдачі сенсора
Залиште затримку 35 секунд:
```python
time.sleep(35)  # Сенсор завершиться з тайм-аутом
```

## Очікувані результати

1. **Граф DAG** показує правильні залежності та розгалуження
2. **Таблиця `IllyaF_medal_counts`** створюється з правильною структурою
3. **Один з трьох шляхів** виконується залежно від випадкового вибору
4. **Дані вставляються** у таблицю з правильним типом медалі та кількістю
5. **Сенсор успішно спрацьовує** при 25-секундній затримці
6. **Сенсор завершується з помилкою** при 35-секундній затримці

## Автор

Illya Fefelov - GoIT Data Engineering Course
