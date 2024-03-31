from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import logging
import json

# DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Inicialização da DAG
dag = DAG(
    'create_json_files_dag',
    default_args=default_args,
    description='A DAG to migrate student data into Snowflake',
    schedule_interval=timedelta(days=1),
)
# Função para carregar e validar dados do students.json
def load_and_validate_students():
    try:
        with open('/tmp/students.json') as f:
            students_data = json.load(f)
        
        # Implementar logging
        logging.info("Data loaded from students.json: %s", students_data)

        # Implementar validação dos dados
        for student in students_data['students']:
            if 'student_id' not in student or 'name' not in student or 'grades' not in student:
                raise ValueError("Invalid data format in students.json")
        
        logging.info("Data validation successful for students.json")
        
        # Implementar limpeza de dados (opcional)
        # Por exemplo, converter as notas para inteiros
        for student in students_data['students']:
            for subject, grade in student['grades'].items():
                student['grades'][subject] = int(grade)
        
        logging.info("Data cleaning successful for students.json")
        
        return students_data
    except Exception as e:
        logging.error(f"Error loading, validating, or cleaning students data: {str(e)}")
        raise

# Função para carregar e validar dados do missed_days.json
def load_and_validate_missed_days():
    try:
        with open('missed_days.json') as f:
            missed_days_data = json.load(f)
        
        # Implementar logging
        logging.info("Data loaded from missed_days.json: %s", missed_days_data)

        # Implementar validação dos dados
        for entry in missed_days_data['missed_classes']:
            if 'student_id' not in entry or 'missed_days' not in entry:
                raise ValueError("Invalid data format in missed_days.json")
        
        logging.info("Data validation successful for missed_days.json")
        
        # Implementar limpeza de dados (opcional)
        # Não há necessidade de limpeza neste caso
        
        logging.info("Data cleaning successful for missed_days.json")
        
        return missed_days_data
    except Exception as e:
        logging.error(f"Error loading, validating, or cleaning missed days data: {str(e)}")
        raise

# Função para juntar os datasets e processar os dados
def join_and_process_data(students_data, missed_days_data):
    try:
        # Criar dataframes a partir dos dados
        students_df = pd.DataFrame(students_data['students'])
        missed_days_df = pd.DataFrame(missed_days_data['missed_classes'])
        
        # Juntar os dataframes usando o student_id como chave
        merged_df = pd.merge(students_df, missed_days_df, on='student_id', how='left')
        
        # Implementar logging
        logging.info("Merged dataframe:\n%s", merged_df)

        # Implementar limpeza e transformação de dados (opcional)
        # Não há necessidade de limpeza ou transformação adicional neste caso
        
        logging.info("Data cleaning and transformation successful")
        
        # Salvar os dados processados em um arquivo CSV
        merged_df.to_csv('final_merged.csv', index=False)
        
        logging.info("Processed data saved to CSV")
    except Exception as e:
        logging.error(f"Error processing data: {str(e)}")
        raise

# Definir tarefas
load_and_validate_students_task = PythonOperator(
    task_id='load_and_validate_students',
    python_callable=load_and_validate_students,
    dag=dag,
)

load_and_validate_missed_days_task = PythonOperator(
    task_id='load_and_validate_missed_days',
    python_callable=load_and_validate_missed_days,
    dag=dag,
)

join_and_process_data_task = PythonOperator(
    task_id='join_and_process_data',
    python_callable=join_and_process_data,
    op_kwargs={'students_data': load_and_validate_students(), 'missed_days_data': load_and_validate_missed_days()},
    dag=dag,
)

# Definir dependências entre as tarefas
load_and_validate_students_task >> load_and_validate_missed_days_task >> join_and_process_data_task
