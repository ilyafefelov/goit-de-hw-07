# GoIT Airflow Upload Quick Guide

## Access Information
- **Airflow URL**: https://airflow.goit.global/home
- **Username**: airflow
- **Password**: airflow
- **GitHub Repository**: https://github.com/ilyafefelov/goit-de-hw-07

## Upload Steps

### 1. Login to Airflow
1. Open https://airflow.goit.global/home
2. Login with airflow/airflow

### 2. Upload DAG File
**Primary file to upload**: `olympic_medals_dag_v2.py`

**Methods** (try in order):
1. **Git Integration**: Ask GoIT if they sync from GitHub automatically
2. **Web Upload**: Look for Admin → Files or similar section
3. **Manual**: Contact GoIT administrators with GitHub link

### 3. Configure MySQL Connection
**Navigation**: Admin → Connections → mysql_default (or create new)

**Connection Parameters**:
```
Connection ID: mysql_default
Connection Type: MySQL
Host: 217.61.57.46
Database: neo_data
Login: neo_data_admin
Password: Proyahaxuqithab9oplp
Port: 3306
```

### 4. Test Scenarios

#### Scenario 1: Success Test (25-second delay)
1. Edit line 140 in DAG: `time.sleep(25)`
2. Upload updated file
3. Enable DAG and trigger manually
4. Verify sensor passes

#### Scenario 2: Failure Test (35-second delay) 
1. Edit line 140 in DAG: `time.sleep(35)`
2. Upload updated file  
3. Enable DAG and trigger manually
4. Verify sensor fails after 30 seconds

### 5. Expected Results
- **Medal counts**: Bronze: 1004, Silver: 472, Gold: 495
- **Table**: `IllyaF_medal_counts` with new records
- **DAG name**: `olympic_medals_processing_v2`

### 6. Screenshots Needed
1. Graph View - successful run
2. Graph View - sensor failure
3. Task logs
4. MySQL query results

## Quick SQL Check
```sql
SELECT id, medal_type, count, created_at 
FROM IllyaF_medal_counts 
ORDER BY created_at DESC 
LIMIT 10;
```

## Files Ready for Upload
✅ olympic_medals_dag_v2.py (main DAG file)
✅ requirements.txt
✅ All documentation
✅ GitHub repository synced
