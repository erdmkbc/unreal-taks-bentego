############################## İmport işlemleri ############################## 
# DAG komponentinin import edilmesi ve scheduler time kontrolü için time paketlerinin import edilmesi
from datetime import timedelta
from airflow import DAG
# airflow metadb'ye bağlanılması 
import sqlite3
# yapılacak dosya işlemleri için bash komutları, airflow komutları için taskların bash komutları
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
# python taskları dönüşü için 
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# metadb verilerinin okunması için pandasın import edilmesi 
import pandas as pd 
###############################################################################


########################## Task tanımı ########################## 
# 1- Task Şeması
# son tarihin basılması taskının oluşturulması  
# airflow daglerinin listelenmesi taskının oluşturulması 
# airflow dagleri içinde ki dag de ki taskların listelenmesi taskının oluşturulması 
# airflow db'sine sorgu atılımı rapor çıkarılması
#   - fail taskların tespit edilmesi - genel - DONE
#   - Son tasklar da ve genel kaç fail kaç succes task var çıkarılması - DONE 
#   - Son tasklar da ki fail taskların adının çıkarılması - DONE

# 2- Txt ile email eki oluşturulması - DONE 
# Txt'lere append olarak eklendi genel zamanlarda ki loglamaların historik bir veri sağlanması açısında sağlanması adına
# Txt'lere overwrite yazılarak sürekli güncel kalınması sağlandı

####################### Notlar #######################
# 1- Airflow connections şeması üzerinden airflow_db nin mysql ile bağlantısı olduğu gözlemlendi.
# 2 - Astronomer üzerinde airflow backendine restapi üzerinde request atılarak gerçekleştirildiği gözlemlendi
######################################################

############################## Code Section ############################## 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

################### task loglarının tablolarının oluşturulması ################### 
def fail_tasks_table_prepration(*args):
    
    # metadb ile bağlanıtının kurulması 
    con = sqlite3.connect("/home/train/airflow/airflow.db")

    ######### fail taskların tespit edilmesi - genel
    failed_tasks = pd.read_sql('''SELECT 
                                    task_id,
                                    dag_id,
                                    execution_date,  
                                    FROM task_instance
                                    WHERE state = 'failed' ''', con = con)
    
    # email eki oluşturularak txt'ye yazılması
    with open('fail_logs', 'a') as f:
        
        failed_tasks_string = failed_tasks.to_string(header=False, index=False)
        f.write(failed_tasks_string)
    
    ######## en son zamanda ki fail tasklarının çıkarılımı
        failed_tasks_last_time =  pd.read_sql('''SELECT 
                                                    task_id,
                                                    dag_id,
                                                    execution_date
                                                        FROM task_instance
                                                    WHERE state = 'failed' AND 
                                                    execution_date BETWEEN datetime('now', 'start of day') AND datetime('now', 'localtime') ''', con = con)
    
    # email eki oluşturularak txt'ye yazılması
    with open('fail_logs_last_time', 'w') as f:
        
        failed_tasks_string = failed_tasks.to_string(header=False, index=False)
        f.write(failed_tasks_last_time)

    ######## succes ve fail de olan taskların sayıları - genel ve son zaman
    failed_tasks_last_time =  pd.read_sql('''SELECT 
                                                    COUNT(state) as task_state_cnt,
                                                    task_id,
                                                    dag_id,
                                                    execution_date,
                                                    state,
                                                    'general' as analyz_type
                                                        FROM task_instance 
                                                    GROUP BY 2,3,4,5,6
                                                    UNION ALL 
                                            SELECT 
                                                    COUNT(state) as task_state_cnt,
                                                    task_id,
                                                    dag_id,
                                                    execution_date,
                                                    state,
                                                    'last_time' as analyz_type
                                                        FROM task_instance 
                                                    GROUP BY 2,3,4,5,6
                                                    WHERE execution_date BETWEEN datetime('now', 'start of day') AND datetime('now', 'localtime')   
                                                    GROUP BY 2,3,4''', con = con)
    
    # email eki oluşturularak txt'ye yazılması
    with open('fail_logs_aggregation', 'a') as f:
        
        failed_tasks_string = failed_tasks.to_string(header=False, index=False)
        f.write(failed_tasks_last_time)


######################## Log analizi fonksiyonun dage takılması ############################## 
dag = DAG(
    'task_logs_DAG',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
)

task_print_date = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

task_list_dags = BashOperator(
    task_id = 'list_dags',
    depends_on_past=False,
    bash_command='airflow list_dags',
    dag=dag
)

task_list_tasks = BashOperator(
    task_id = 'list_tasks',
    depends_on_past=False,
    bash_command='airflow list_tasks tutorial',
    dag=dag
)

task_logs_attachs_analyz = PythonOperator(
    task_id = 'analyz_logs',
    depends_on_past=False,
    python_callable=fail_tasks_table_prepration,
    dag = dag
)

[task_print_date >> task_list_dags >> task_list_tasks] >> task_logs_attachs_analyz




















