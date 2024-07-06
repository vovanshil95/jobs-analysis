from datetime import datetime, timedelta
import os
import requests
import time 


from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2


default_args = {
    'owner': 'vladimir',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

сovering_letter_text = '''Здравствуйте! Прошу рассмотреть моё резюме на роль Data Scientist в вашу компанию.

Краткое содержание: 
- опыт работы: 2.5 года
- зарплатные ожидания: от 200 т.р.
- основные компетенции: Python, Machine Learning, Data Science, SQL
- есть опыт работы с технологиями Spark, Airflow, Docker
- есть опыт работы с нейросетями (NLP, CV, CNN, Transformers)
- есть опыт создания микросервиса на FastApi
- отличное знание мат. статистики'''


def get_postgres():
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = os.environ.get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')
    con = AIRFLOW__DATABASE__SQL_ALCHEMY_CONN.split('//')[1]
    user, con = con.split(':')
    password, con = con.split('@')
    host, database = con.split('/')

    return {'user': user, 'password': password, 'host': host, 'database': database}


def get_vacancies_from_db(ti):
    sql_config = get_postgres()
        
    with psycopg2.connect(**sql_config) as connection:
        cur = connection.cursor()
        cur.execute('select * from vacancy where (not responded) and (not has_test)')
        result = cur.fetchall()
        not_applied_ids = [row[0] for row in result]
    ti.xcom_push(key='not_applied_ids', value=not_applied_ids)


def apply_to_vacancies(resume_id, ti):

    vacancy_ids = ti.xcom_pull(task_ids='get_vacancies_from_db', key='not_applied_ids')

    HH_ACCESS_TOKEN = os.environ['HH_ACCESS_TOKEN']
    APP_NAME = 'my_super_applier_app'
    MAX_DAY_APPLY_COUNT = 200

    click_url = 'https://api.hh.ru/negotiations'

    headers = {
            'Authorization': f'Bearer {HH_ACCESS_TOKEN}',
            'HH-User-Agent': APP_NAME
        }
    
    successful_responses = []

    for vacancy_id in vacancy_ids[:MAX_DAY_APPLY_COUNT]:
        params = {
            'vacancy_id': vacancy_id,
            'resume_id': resume_id,
        }

        try:
            response = requests.post(
                click_url,
                headers=headers,
                params=params
            )
            if str(response.status_code)[0] == '2':
                successful_responses.append(vacancy_id)
            else:
                print(f'not successfull status code ({response.status_code}) while applying to {vacancy_id}')
                print(f'response dict: {response.json()}')
        except Exception as e:
            print(f'Exception while applying to {vacancy_id}', e)

        time.sleep(3)
    
    ti.xcom_push(key='successful_responses', value=successful_responses)


def change_vacancies_status(ti):
    vacancy_ids = ti.xcom_pull(task_ids='apply_to_vacancies', key='successful_responses')

    sql_config = get_postgres()

    with psycopg2.connect(**sql_config) as connection:
        cur = connection.cursor()
        cur.execute(f'update vacancy set responded = True where id in ({', '.join(vacancy_ids)})')
    

with DAG(
    dag_id='apply', 
    default_args=default_args,
    description='obtain vaccines from the database and apply for them',
    start_date=datetime(2024, 6, 30),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id='get_vacancies_from_db',
        python_callable=get_vacancies_from_db
    )
    task2 = PythonOperator(
        task_id='apply_to_vacancies',
        python_callable=apply_to_vacancies,
        op_kwargs={'resume_id': 'a57b16adff0922d1da0039ed1f796374744b4f',
                   'message': сovering_letter_text}
    )
    task3 = PythonOperator(
        task_id='change_vacancies_status',
        python_callable=change_vacancies_status
    )

    task1 >> task2
    task2 >> task3
