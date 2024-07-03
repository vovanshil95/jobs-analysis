from datetime import datetime, timedelta
import requests
import os
import random
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import psycopg2


default_args = {
    'owner': 'vladimir',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

   

def in_(sub_strs, str_):
    for sub_str in sub_strs:
        if sub_str.lower() in str_.lower():
            return True
    return False


def estimate_salary(el, rates):
    if el['salary'] is not None:
        if el['salary_from'] is None:
            salary_amount = (el['salary_to'] * 0.66 + el['salary_to']) / 2
        elif el['salary_to'] is None:
            salary_amount = (el['salary_from'] + el['salary_from'] * 1.5) / 2
        else:
            salary_amount = (el['salary_from'] + el['salary_to']) / 2
        salary_amount /= rates[el['currency']]
    
    else:
        salary_amount = None
    return salary_amount


def get_vacancies_df(ids, rates):

    jobs = []
    for id in ids:
        time.sleep(1)
        job = requests.get(f'https://api.hh.ru/vacancies/{id}').json()
        jobs.append(job)

    df = pd.DataFrame(jobs)

    df = df.drop([
        'billing_type',
        'relations',
        'insider_interview',
        'contacts',
        'branded_description',
        'vacancy_constructor_template',
        'accept_handicapped',
        'accept_kids',
        'archived',
        'response_url',
        'code',
        'hidden',
        'quick_responses_allowed',
        'driver_license_types',
        'accept_incomplete_resumes',
        'created_at',
        'initial_created_at',
        'negotiations_url',
        'suitable_resumes_url',
        'apply_alternate_url',
        'alternate_url',
        'languages',
        'approved',
        'working_days',
        'test',
        'working_time_intervals',
        'working_time_modes',
        'specializations',
        'allow_messages'], axis=1)

    df.replace(np.nan, None, inplace=True)

    df.area = df.area.apply(lambda el: el['name'])
    df.type = df.type.apply(lambda el: el['name'])
    df.professional_roles = df.professional_roles.apply(lambda el: el[0]['name'])
    df.experience = df.experience.apply(lambda el: el['name'])
    df.schedule = df.schedule.apply(lambda el: el['name'])
    df.employment = df.employment.apply(lambda el: el['name'])
    df.employer = df.employer.apply(lambda el: el['name'])
    df.department = df.department.apply(lambda el: el['name'] if el is not None else None)
    df.key_skills = df.key_skills.apply(lambda el: [skill['name'] for skill in el])

    df['salary_from'] = df.salary.apply(lambda el: el['from'] if el is not None else None)
    df['salary_to'] = df.salary.apply(lambda el: el['to'] if el is not None else None)
    df['currency'] = df.salary.apply(lambda el: el['currency'] if el is not None else None)
    df.replace(np.nan, None, inplace=True)
    df['rur_salary_avg'] = df.apply(lambda el: estimate_salary(el, rates), axis=1)
    df['rur_salary_from'] = df.apply(lambda el: el['salary_from'] / rates[el['currency']] if el['salary_from'] is not None else None, axis=1)
    df['rur_salary_to'] = df.apply(lambda el: el['salary_to'] / rates[el['currency']] if el['salary_to'] is not None else None, axis=1)
    df = df.drop(['salary'], axis=1)

    df['grade'] = df['name']\
    .apply(lambda el: 'lead' if 'lead' in el.lower()
    else 'middle_plus' if ('middle' in el.lower() and 'senior' in el.lower()) or 'middle+' in el.lower()
    else 'senior' if 'senior' in el.lower()
    else 'middle' if 'middle' in el.lower()
    else 'junior' if 'junior' in el.lower()
    else 'intern' if 'intern' in el.lower() or 'стажёр' in el.lower() or 'стажер' in el.lower()
    else None)

    df['city'] = df.address.apply(lambda el: el['city'] if el is not None else None)
    df['street'] = df.address.apply(lambda el: el['street'] if el is not None else None)
    df['building'] = df.address.apply(lambda el: el['building'] if el is not None else None)
    df['lat'] = df.address.apply(lambda el: el['lat'] if el is not None else None)
    df['lng'] = df.address.apply(lambda el: el['lng'] if el is not None else None)
    df['full_address'] = df.address.apply(lambda el: el['raw'] if el is not None else None)
    df = df.drop('address', axis=1)

    df['dt'] = datetime.now()

    df = df.rename(columns={'name': 'name_',
                            'type': 'type_',
                            'professional_roles': 'professional_role',
                            'description': 'description_'})

    df.id = df.id.astype(int)

    return df


def get_postgres():
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = os.environ.get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')
    con = AIRFLOW__DATABASE__SQL_ALCHEMY_CONN.split('//')[1]
    user, con = con.split(':')
    password, con = con.split('@')
    host, database = con.split('/')

    return {'user': user, 'password': password, 'host': host, 'database': database}


def df_to_db(df_, cur, table):
    df_ = df_.replace(np.nan, None)
    columns = '(' + ', '.join(df_.columns) + ')'
    values = []
    for i in range(len(df_)):
        values.append('(' + ', '.join(['NULL' if el is None
                      else  "'" + str(el) + "'" 
                      if not isinstance(el, np.int64)
                      and not isinstance(el, np.float64) 
                      and not isinstance(el, np.bool_)
                      and not isinstance(el, int)
                      and not isinstance(el, float)
                      else str(el)
          for el in df_.iloc[i].tolist()]) + ')')
        
    values = ',\n'.join(values)

    cur.execute(f'insert into {table} {columns} values {values}')


def get_currency_rates(ti):
    API_KEY = os.environ.get('EXCHANGERATE_API_KEY')
    rates = requests.get(f'https://v6.exchangerate-api.com/v6/{API_KEY}/latest/RUB').json()['conversion_rates']
    rate = rates.pop('RUB')
    rates.update({'RUR': rate})
    ti.xcom_push(key='rates', value=rates)


def get_hh_vacancy_ids(search_request_text, key_words, ti):
    jobs = []
    for i in range(20):
        items = requests.get('https://api.hh.ru/vacancies', params={'page': i, 'per_page': 100, 'text': search_request_text}).json()['items']
        jobs.extend(items)
        if len(items) < 100:
            break
    
    job_ids = set([job['id'] for job in jobs if in_(key_words, job['name'])])
    ti.xcom_push(key='job_ids', value=job_ids)


def add_vacancies_to_db(ti):

    ids_from_hh = ti.xcom_pull(task_ids='get_hh_vacancy_ids', key='job_ids')
    rates = ti.xcom_pull(task_ids='get_currency_rates', key='rates')

    sql_config = get_postgres()

    with psycopg2.connect(**sql_config) as connection:
        cur = connection.cursor()

        cur.execute("""SELECT id 
                       FROM vacancy 
                       where type_ = 'Рекламная' 
                       or type_ = 'Открытая'""")
        result = cur.fetchall()

        ids_from_db = set([row[0] for row in result])
        new_ids = ids_from_hh - ids_from_db
        unfound_ids = ids_from_db - ids_from_hh

        ti.xcom_push(key='unfound_ids', value=unfound_ids)

        df = get_vacancies_df(new_ids, rates)

        df['responded'] = False

        df_to_db(df.drop('key_skills', axis=1), cur, 'vacancy')

        cur.execute('SELECT id, name_ FROM key_skill')
        result = cur.fetchall()

        key_skills_from_db_df =  pd.DataFrame([[row[0], row[1]] for row in result], columns=['id', 'name_'])
        key_skills_from_db = set(key_skills_from_db_df['name_'])

        key_skills = df.key_skills.tolist()
        key_skills_from_hh = {skill for sublist in key_skills for skill in sublist}

        new_key_skills = key_skills_from_hh - key_skills_from_db

        new_skills_df = pd.DataFrame({'id': [random.randint(0, int(10 ** 9)) for _ in new_key_skills], 'name_': list(new_key_skills)})
        
        df_to_db(new_skills_df, cur, 'key_skill')

        skill_vacancy = {'skill_id': [], 'vacancy_id': []}

        all_skills = pd.concat([new_skills_df, key_skills_from_db_df])
        for vacancy_id in new_ids:
            skills = df[df.id == int(vacancy_id)].key_skills.iloc[0]
            for skill in skills:
                skill_id = all_skills[all_skills.name_ == skill].id.iloc[0]
                skill_vacancy['skill_id'].append(skill_id)
                skill_vacancy['vacancy_id'].append(int(vacancy_id))

        skill_vacancy = pd.DataFrame(skill_vacancy)

        df_to_db(skill_vacancy, cur, 'skill_vacancy')

        connection.commit()


def change_jobs_status(ti):
    unfound_ids = ti.xcom_pull(task_ids='add_vacancies_to_db', key='unfound_ids')
    rates = ti.xcom_pull(task_ids='get_currency_rates', key='rates')

    if len(unfound_ids) > 0:
        unfound_jobs = get_vacancies_df(unfound_ids, rates)
        unfound_jobs.sort_values(by='id')
        
        sql_config = get_postgres()
        
        with psycopg2.connect(**sql_config) as connection:
            cur = connection.cursor()

            cur.execute(f'select responded from vacancy where id in ({", ".join(map(str, unfound_ids))}) order by id')
            result = cur.fetchall()
            responded = pd.Series([row[0] for row in result])
            unfound_jobs['responded'] = responded

            cur.execute(f'delete from vacancy where id in ({", ".join(map(str, unfound_ids))})')
            df_to_db(unfound_jobs.drop('key_skills', axis=1), cur, 'vacancy')
            connection.commit()


with DAG(
    dag_id='get_vacancies', 
    default_args=default_args,
    description='receives vacancies from hh and writes them to the database',
    start_date=datetime(2024, 6, 30),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id='get_currency_rates',
        python_callable=get_currency_rates
    )
    task2 = PythonOperator(
        task_id='get_hh_vacancy_ids',
        python_callable=get_hh_vacancy_ids,
        op_kwargs={'search_request_text': 'Data Scientist', 
                   'key_words': ['data',
                                 'ml',
                                 'machine learning',
                                 'nlp',
                                 'cv',
                                 'computer vision',
                                 'дата',
                                 'данных',
                                 'компьютерное зрение',
                                 'LLM',
                                 'recsys']}
    )
    task3 = PythonOperator(
        task_id='add_vacancies_to_db',
        python_callable=add_vacancies_to_db
    )
    task4 = PythonOperator(
        task_id='change_jobs_status',
        python_callable=change_jobs_status
    )

    task1 >> task3
    task2 >> task3
    task3 >> task4
